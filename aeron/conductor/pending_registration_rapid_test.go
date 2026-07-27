//go:build rapid

package conductor_test

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/aeron/cnc"
	"github.com/warrenguy/heron/aeron/conductor"
	"github.com/warrenguy/heron/aeron/logbuffer"
)

var errRapidApplySubscriptionReady = errors.New("conductor_test: apply subscription ready failed")

var rapidPendingKinds = []conductor.PendingKind{
	conductor.PendingKindNextSessionID,
	conductor.PendingKindOperationSuccess,
	conductor.PendingKindAddPublication,
	conductor.PendingKindAddSubscription,
	conductor.PendingKindAddExclusivePublication,
	conductor.PendingKindRemovePublication,
	conductor.PendingKindRemoveSubscription,
}

func newConductorForRapid(t *testing.T, rt *rapid.T) *conductor.Conductor {
	t.Helper()
	rt.Helper()

	conductorInstance, nanoNs := newConductorForMutableNanoTest(t, func(error) {}, 0)
	conductorInstance.SeedServiceTimestamps(*nanoNs)
	return conductorInstance
}

func rapidDriverRejectedResponse(correlationID int64) conductor.ErrorResponse {
	return conductor.ErrorResponse{
		OffendingCommandCorrelationID: correlationID,
		ErrorCode:                     11,
		ErrorMessage:                  "rejected",
	}
}

type pendingRapidSeen struct {
	kind     conductor.PendingKind
	streamID int32
	channel  string
}

func registerPendingForRapid(
	rt *rapid.T,
	conductorInstance *conductor.Conductor,
	kind conductor.PendingKind,
	correlationID int64,
	streamID int32,
	channel string,
) error {
	rt.Helper()

	return registerPendingKindWithDeadlineForTest(
		conductorInstance,
		kind,
		correlationID,
		streamID,
		channel,
		time.Second,
	)
}

func TestConductor_RegisterPending_correlationUniqueness_rapid(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(rt *rapid.T) {
		conductorInstance := newConductorForRapid(t, rt)
		count := rapid.IntRange(1, 32).Draw(rt, "count")
		seen := make(map[int64]pendingRapidSeen, count)

		for range count {
			correlationID := drawPendingRapidUniqueCorrelationID(
				rt,
				rapidMapContains(seen),
				"correlationID",
			)

			kind := rapid.SampledFrom(rapidPendingKinds).Draw(rt, "kind")
			var streamID int32
			var channel string
			if conductor.AddPendingKindForTest(kind) {
				streamID = rapid.Int32().Draw(rt, "streamID")
				channel = rapid.String().Draw(rt, "channel")
			}
			require.NoError(rt, registerPendingForRapid(
				rt,
				conductorInstance,
				kind,
				correlationID,
				streamID,
				channel,
			))
			seen[correlationID] = pendingRapidSeen{
				kind:     kind,
				streamID: streamID,
				channel:  channel,
			}
		}

		for correlationID, entry := range seen {
			got, ok := conductorInstance.PendingResult(correlationID)
			require.True(rt, ok)
			require.Equal(rt, correlationID, got.CorrelationID)
			require.Equal(rt, entry.kind, got.Kind)
			if conductor.AddPendingKindForTest(entry.kind) {
				require.Equal(rt, entry.streamID, got.StreamID)
				require.Equal(rt, entry.channel, got.Channel)
			}
		}

		for correlationID, entry := range seen {
			err := registerPendingForRapid(
				rt,
				conductorInstance,
				entry.kind,
				correlationID,
				rapid.Int32().Draw(rt, "dupStreamID"),
				rapid.String().Draw(rt, "dupChannel"),
			)
			require.ErrorIs(rt, err, conductor.ErrPendingAlreadyRegistered)
			require.EqualError(rt, err, "conductor: correlation already pending")
		}
	})
}

const pendingRapidBaseNowNs int64 = 1_000_000_000

type pendingRapidMapEntry struct {
	kind          conductor.PendingKind
	deadlineNs    int64
	err           error
	driverErrored bool
}

type pendingRapidMapModel struct {
	entries               map[int64]pendingRapidMapEntry
	receivedCorrelationID int64
	activeCorrelationID   int64
	nowNs                 int64
}

func rapidMapKeys[K comparable, V any](values map[K]V) []K {
	keys := make([]K, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	return keys
}

func rapidMapContains[K comparable, V any](values map[K]V) func(K) bool {
	return func(key K) bool {
		_, exists := values[key]
		return exists
	}
}

func drawRapidCorrelationID(
	rt *rapid.T,
	label string,
	reject func(int64) bool,
) int64 {
	rt.Helper()

	for {
		correlationID := rapid.Int64().Draw(rt, label)
		if reject(correlationID) {
			continue
		}
		return correlationID
	}
}

func drawPendingRapidUniqueCorrelationID(
	rt *rapid.T,
	isTaken func(int64) bool,
	label string,
) int64 {
	rt.Helper()

	return drawRapidCorrelationID(rt, label, func(correlationID int64) bool {
		return correlationID == conductor.NullCorrelationID || isTaken(correlationID)
	})
}

func drawRapidAbsentCorrelationID(
	rt *rapid.T,
	label string,
	isPresent func(int64) bool,
) int64 {
	rt.Helper()

	return drawRapidCorrelationID(rt, label, func(correlationID int64) bool {
		return correlationID == conductor.NullCorrelationID || isPresent(correlationID)
	})
}

func drawRapidInt64Key(rt *rapid.T, keys []int64, label string) int64 {
	rt.Helper()

	return keys[rapid.IntRange(0, len(keys)-1).Draw(rt, label)]
}

func drawPendingRapidExistingCorrelationID(
	rt *rapid.T,
	model pendingRapidMapModel,
	label string,
) (int64, bool) {
	rt.Helper()

	if len(model.entries) == 0 {
		return 0, false
	}
	return drawRapidInt64Key(rt, rapidMapKeys(model.entries), label), true
}

func pendingRapidDeadlineOffset(rt *rapid.T, label string) int64 {
	rt.Helper()

	return rapid.Int64Range(10_000, 50_000_000).Draw(rt, label)
}

func registerPendingRapidMapEntry(
	rt *rapid.T,
	conductorInstance *conductor.Conductor,
	model *pendingRapidMapModel,
	correlationID int64,
	kind conductor.PendingKind,
) {
	rt.Helper()

	deadlineNs := model.nowNs + pendingRapidDeadlineOffset(rt, "deadlineOffset")
	timeout := time.Duration(deadlineNs - model.nowNs)
	var streamID int32
	var channel string
	if conductor.AddPendingKindForTest(kind) {
		streamID = rapid.Int32().Draw(rt, "streamID")
		channel = rapid.String().Draw(rt, "channel")
	}
	require.NoError(
		rt,
		registerPendingKindWithDeadlineForTest(
			conductorInstance,
			kind,
			correlationID,
			streamID,
			channel,
			timeout,
		),
	)
	model.entries[correlationID] = pendingRapidMapEntry{
		kind:       kind,
		deadlineNs: deadlineNs,
	}
}

func rapidPendingPollMarked(receivedCorrelationID, correlationID int64) bool {
	return receivedCorrelationID == correlationID
}

func rapidPendingPollCompletes(receivedCorrelationID, correlationID int64, mapResolved bool) bool {
	return rapidPendingPollMarked(receivedCorrelationID, correlationID) || mapResolved
}

func clearReceivedCorrelationIDIf(receivedCorrelationID *int64, correlationID int64) {
	if *receivedCorrelationID == correlationID {
		*receivedCorrelationID = conductor.NullCorrelationID
	}
}

func receivedCorrelationMatches(
	conductorInstance *conductor.Conductor,
	correlationID int64,
) bool {
	return conductorInstance.ReceivedCorrelationID() == correlationID
}

func applyPendingRapidCancel(model *pendingRapidMapModel, correlationID int64) {
	if _, ok := model.entries[correlationID]; !ok {
		return
	}
	delete(model.entries, correlationID)
	clearReceivedCorrelationIDIf(&model.receivedCorrelationID, correlationID)
}

func pendingRapidMapEntryResolved(entry pendingRapidMapEntry) bool {
	return entry.err != nil || entry.driverErrored
}

func applyPendingRapidPoll(model *pendingRapidMapModel, correlationID int64) {
	entry, ok := model.entries[correlationID]
	if !ok {
		return
	}
	marked := rapidPendingPollMarked(model.receivedCorrelationID, correlationID)
	mapResolved := pendingRapidMapEntryResolved(entry)
	if !rapidPendingPollCompletes(model.receivedCorrelationID, correlationID, mapResolved) {
		return
	}
	delete(model.entries, correlationID)
	if marked {
		clearReceivedCorrelationIDIf(&model.receivedCorrelationID, correlationID)
	}
}

func applyPendingRapidMapDriverError(model *pendingRapidMapModel, correlationID int64) {
	entry, ok := model.entries[correlationID]
	if !ok {
		return
	}
	entry.driverErrored = true
	model.entries[correlationID] = entry
	model.receivedCorrelationID = correlationID
}

func applyPendingRapidClear(model *pendingRapidMapModel) {
	clear(model.entries)
	model.receivedCorrelationID = conductor.NullCorrelationID
	model.activeCorrelationID = conductor.NullCorrelationID
}

func syncPendingRapidMapAfterDoWork(
	conductorInstance *conductor.Conductor,
	model *pendingRapidMapModel,
) {
	for correlationID, entry := range model.entries {
		got, ok := conductorInstance.PendingResult(correlationID)
		if !ok {
			delete(model.entries, correlationID)
			continue
		}
		entry.deadlineNs = got.DeadlineNs
		entry.err = got.Err
		var driverErr *conductor.DriverRegistrationError
		entry.driverErrored = errors.As(got.Err, &driverErr)
		model.entries[correlationID] = entry
	}
	model.receivedCorrelationID = conductorInstance.ReceivedCorrelationID()
}

func assertPendingRapidMapModel(
	rt *rapid.T,
	conductorInstance *conductor.Conductor,
	model pendingRapidMapModel,
) {
	rt.Helper()

	for correlationID, entry := range model.entries {
		got, ok := conductorInstance.PendingResult(correlationID)
		require.True(rt, ok, "correlation %d should remain pending", correlationID)
		require.Equal(rt, entry.kind, got.Kind)
		require.Equal(rt, entry.deadlineNs, got.DeadlineNs)
		switch {
		case entry.driverErrored:
			var driverErr *conductor.DriverRegistrationError
			require.ErrorAs(rt, got.Err, &driverErr)
			require.Equal(rt, correlationID, driverErr.CorrelationID)
		case entry.err != nil:
			require.ErrorIs(rt, got.Err, entry.err)
		default:
			require.NoError(rt, got.Err)
		}
	}
	require.Equal(rt, model.receivedCorrelationID, conductorInstance.ReceivedCorrelationID())
	require.Equal(rt, model.activeCorrelationID, conductorInstance.ActiveCorrelationID())
	if model.receivedCorrelationID != conductor.NullCorrelationID {
		_, ok := model.entries[model.receivedCorrelationID]
		require.True(rt, ok, "received correlation should remain pending")
	}
}

func newConductorForPendingRapidMap(
	t *testing.T,
	rt *rapid.T,
) (*conductor.Conductor, *int64, *pendingRapidMapModel) {
	t.Helper()
	rt.Helper()

	conductorInstance, nanoNs := newConductorForMutableNanoTest(t, func(error) {}, 0)
	*nanoNs = pendingRapidBaseNowNs
	conductorInstance.SeedServiceTimestamps(pendingRapidBaseNowNs)
	return conductorInstance, nanoNs, &pendingRapidMapModel{
		entries:               make(map[int64]pendingRapidMapEntry),
		receivedCorrelationID: conductor.NullCorrelationID,
		activeCorrelationID:   conductor.NullCorrelationID,
		nowNs:                 pendingRapidBaseNowNs,
	}
}

type pendingRapidMapOpKind int

const (
	pendingRapidMapOpRegister pendingRapidMapOpKind = iota
	pendingRapidMapOpCancel
	pendingRapidMapOpPoll
	pendingRapidMapOpAdvanceExpire
	pendingRapidMapOpSetActive
	pendingRapidMapOpClear
	pendingRapidMapOpDriverError
)

func runPendingRapidMapOps(
	t *testing.T,
	rt *rapid.T,
	conductorInstance *conductor.Conductor,
	nanoNs *int64,
	model *pendingRapidMapModel,
	seen map[int64]struct{},
	step int,
) {
	rt.Helper()

	ops := []pendingRapidMapOpKind{
		pendingRapidMapOpRegister,
		pendingRapidMapOpCancel,
		pendingRapidMapOpPoll,
		pendingRapidMapOpAdvanceExpire,
		pendingRapidMapOpSetActive,
	}
	if len(model.entries) > 0 {
		ops = append(ops, pendingRapidMapOpClear, pendingRapidMapOpDriverError)
	}
	switch rapid.SampledFrom(ops).Draw(rt, fmt.Sprintf("op%d", step)) {
	case pendingRapidMapOpRegister:
		correlationID := drawPendingRapidUniqueCorrelationID(
			rt,
			rapidMapContains(seen),
			fmt.Sprintf("registerCorrelationID%d", step),
		)
		seen[correlationID] = struct{}{}
		kind := rapid.SampledFrom(rapidPendingKinds).Draw(rt, fmt.Sprintf("registerKind%d", step))
		registerPendingRapidMapEntry(rt, conductorInstance, model, correlationID, kind)

	case pendingRapidMapOpCancel:
		if rapid.Bool().Draw(rt, fmt.Sprintf("cancelAbsent%d", step)) {
			correlationID := drawRapidAbsentCorrelationID(
				rt,
				fmt.Sprintf("cancelAbsentCorrelationID%d", step),
				rapidMapContains(model.entries),
			)
			conductorInstance.CancelPending(correlationID)
			return
		}
		correlationID, ok := drawPendingRapidExistingCorrelationID(
			rt,
			*model,
			fmt.Sprintf("cancelCorrelationID%d", step),
		)
		if !ok {
			return
		}
		conductorInstance.CancelPending(correlationID)
		applyPendingRapidCancel(model, correlationID)

	case pendingRapidMapOpPoll:
		if rapid.Bool().Draw(rt, fmt.Sprintf("pollAbsent%d", step)) {
			correlationID := drawRapidAbsentCorrelationID(
				rt,
				fmt.Sprintf("pollAbsentCorrelationID%d", step),
				rapidMapContains(model.entries),
			)
			done, _, err := conductorInstance.PollPending(correlationID)
			require.False(rt, done)
			require.NoError(rt, err)
			return
		}
		correlationID, ok := drawPendingRapidExistingCorrelationID(
			rt,
			*model,
			fmt.Sprintf("pollCorrelationID%d", step),
		)
		if !ok {
			return
		}
		entry := model.entries[correlationID]
		done, _, err := conductorInstance.PollPending(correlationID)
		wantDone := rapidPendingPollCompletes(
			model.receivedCorrelationID,
			correlationID,
			pendingRapidMapEntryResolved(entry),
		)
		require.Equal(rt, wantDone, done)
		if wantDone {
			switch {
			case entry.driverErrored:
				var driverErr *conductor.DriverRegistrationError
				require.ErrorAs(rt, err, &driverErr)
				require.Equal(rt, correlationID, driverErr.CorrelationID)
				require.EqualError(
					rt,
					err,
					(&conductor.DriverRegistrationError{
						CorrelationID: correlationID,
						ErrorCode:     11,
						Message:       "rejected",
					}).Error(),
				)
			case entry.err != nil:
				require.ErrorIs(rt, err, entry.err)
				require.EqualError(rt, err, "conductor: await response timeout")
			default:
				require.NoError(rt, err)
			}
		} else {
			require.NoError(rt, err)
		}
		applyPendingRapidPoll(model, correlationID)

	case pendingRapidMapOpAdvanceExpire:
		model.nowNs += rapid.Int64Range(0, 40_000_000).Draw(rt, fmt.Sprintf("advanceNs%d", step))
		*nanoNs = model.nowNs
		_ = conductorInstance.DoWork()
		syncPendingRapidMapAfterDoWork(conductorInstance, model)

	case pendingRapidMapOpSetActive:
		if len(model.entries) == 0 {
			model.activeCorrelationID = conductor.NullCorrelationID
			conductorInstance.SetActiveCorrelationID(conductor.NullCorrelationID)
			return
		}
		if rapid.Bool().Draw(rt, fmt.Sprintf("activeNull%d", step)) {
			model.activeCorrelationID = conductor.NullCorrelationID
			conductorInstance.SetActiveCorrelationID(conductor.NullCorrelationID)
			return
		}
		correlationID, _ := drawPendingRapidExistingCorrelationID(
			rt,
			*model,
			fmt.Sprintf("activeCorrelationID%d", step),
		)
		model.activeCorrelationID = correlationID
		conductorInstance.SetActiveCorrelationID(correlationID)

	case pendingRapidMapOpClear:
		conductorInstance.ClearPending()
		applyPendingRapidClear(model)

	case pendingRapidMapOpDriverError:
		correlationID, ok := drawPendingRapidExistingCorrelationID(
			rt,
			*model,
			fmt.Sprintf("driverErrorCorrelationID%d", step),
		)
		if !ok {
			return
		}
		ok = conductorInstance.DriverError(rapidDriverRejectedResponse(correlationID))
		require.True(rt, ok)
		applyPendingRapidMapDriverError(model, correlationID)
	}
}

func TestConductor_pendingRegistry_lifecycle_rapid(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(rt *rapid.T) {
		conductorInstance, nanoNs, model := newConductorForPendingRapidMap(t, rt)
		seen := make(map[int64]struct{})
		seedCount := rapid.IntRange(1, 16).Draw(rt, "seedCount")
		for range seedCount {
			correlationID := drawPendingRapidUniqueCorrelationID(
				rt,
				rapidMapContains(seen),
				"seedCorrelationID",
			)
			seen[correlationID] = struct{}{}
			kind := rapid.SampledFrom(rapidPendingKinds).Draw(rt, "seedKind")
			registerPendingRapidMapEntry(rt, conductorInstance, model, correlationID, kind)
		}

		steps := rapid.IntRange(1, 24).Draw(rt, "steps")
		for step := range steps {
			runPendingRapidMapOps(t, rt, conductorInstance, nanoNs, model, seen, step)
			assertPendingRapidMapModel(rt, conductorInstance, *model)
		}
	})
}

func wantCorrelatedForTest(
	activeCorrelationID int64,
	eventCorrelationID int64,
	pendingContains bool,
) bool {
	if eventCorrelationID == activeCorrelationID &&
		activeCorrelationID != conductor.NullCorrelationID {
		return true
	}
	return pendingContains
}

type correlatedRapidOpKind int

const (
	correlatedRapidOpRegister correlatedRapidOpKind = iota
	correlatedRapidOpCancel
	correlatedRapidOpSetActive
)

func TestDriverEventsAdapter_correlated_gate_rapid(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(rt *rapid.T) {
		conductorInstance := newConductorForRapid(t, rt)
		adapter := conductorInstance.EventsAdapterForTest()
		seen := make(map[int64]struct{})
		pending := make(map[int64]struct{})

		seedCount := rapid.IntRange(0, 8).Draw(rt, "seedCount")
		for range seedCount {
			correlationID := drawPendingRapidUniqueCorrelationID(
				rt,
				rapidMapContains(seen),
				"seedCorrelationID",
			)
			seen[correlationID] = struct{}{}
			kind := rapid.SampledFrom(rapidPendingKinds).Draw(rt, "seedKind")
			require.NoError(rt, registerPendingForRapid(rt, conductorInstance, kind, correlationID, 0, ""))
			pending[correlationID] = struct{}{}
		}

		steps := rapid.IntRange(1, 16).Draw(rt, "steps")
		for step := range steps {
			switch rapid.SampledFrom([]correlatedRapidOpKind{
				correlatedRapidOpRegister,
				correlatedRapidOpCancel,
				correlatedRapidOpSetActive,
			}).Draw(rt, fmt.Sprintf("op%d", step)) {
			case correlatedRapidOpRegister:
				correlationID := drawPendingRapidUniqueCorrelationID(
					rt,
					rapidMapContains(seen),
					fmt.Sprintf("registerCorrelationID%d", step),
				)
				seen[correlationID] = struct{}{}
				kind := rapid.SampledFrom(rapidPendingKinds).Draw(rt, fmt.Sprintf("registerKind%d", step))
				require.NoError(
					rt,
					registerPendingForRapid(rt, conductorInstance, kind, correlationID, 0, ""),
				)
				pending[correlationID] = struct{}{}

			case correlatedRapidOpCancel:
				if len(pending) == 0 {
					break
				}
				correlationID := drawRapidInt64Key(
					rt,
					rapidMapKeys(pending),
					fmt.Sprintf("cancelIdx%d", step),
				)
				conductorInstance.CancelPending(correlationID)
				delete(pending, correlationID)

			case correlatedRapidOpSetActive:
				if len(pending) == 0 {
					conductorInstance.SetActiveCorrelationID(conductor.NullCorrelationID)
					break
				}
				if rapid.Bool().Draw(rt, fmt.Sprintf("activeNull%d", step)) {
					conductorInstance.SetActiveCorrelationID(conductor.NullCorrelationID)
					break
				}
				conductorInstance.SetActiveCorrelationID(
					drawRapidInt64Key(rt, rapidMapKeys(pending), fmt.Sprintf("activeIdx%d", step)),
				)
			}

			eventCorrelationID := rapid.Int64().Draw(rt, fmt.Sprintf("eventCorrelationID%d", step))
			activeCorrelationID := conductorInstance.ActiveCorrelationID()
			_, contains := pending[eventCorrelationID]
			want := wantCorrelatedForTest(activeCorrelationID, eventCorrelationID, contains)
			got := adapter.CorrelatedForTest(activeCorrelationID, eventCorrelationID)
			require.Equal(rt, want, got)
		}
	})
}

type addPendingRapidEntry struct {
	kind           conductor.PendingKind
	present        bool
	readyMarked    bool
	driverErrored  bool
	applyFailed    bool
	timedOut       bool
	registrationID int64
	deadlineNs     int64
}

type addPendingRapidModel struct {
	entries               map[int64]addPendingRapidEntry
	receivedCorrelationID int64
	activeCorrelationID   int64
	nowNs                 int64
}

type addPendingRapidOpKind int

const (
	addPendingRapidOpRegisterPub addPendingRapidOpKind = iota
	addPendingRapidOpRegisterExclusivePub
	addPendingRapidOpRegisterSub
	addPendingRapidOpCompletePubReady
	addPendingRapidOpCompleteExclusivePubReady
	addPendingRapidOpCompleteSubReady
	addPendingRapidOpSetActive
	addPendingRapidOpCancel
	addPendingRapidOpClear
	addPendingRapidOpDriverError
	addPendingRapidOpExpire
	addPendingRapidOpPoll
)

func addPendingRapidEntryCompletable(entry addPendingRapidEntry) bool {
	return entry.present &&
		!entry.readyMarked &&
		!entry.driverErrored &&
		!entry.timedOut
}

func drawAddPendingRapidCompletable(
	model addPendingRapidModel,
	kind conductor.PendingKind,
) (int64, bool) {
	for correlationID, entry := range model.entries {
		if !addPendingRapidEntryCompletable(entry) {
			continue
		}
		if entry.kind == kind {
			return correlationID, true
		}
	}
	return 0, false
}

func drawAddPendingRapidPresent(
	model addPendingRapidModel,
) (int64, bool) {
	for correlationID, entry := range model.entries {
		if entry.present {
			return correlationID, true
		}
	}
	return 0, false
}

func addPendingRapidEntryResolved(entry addPendingRapidEntry) bool {
	if entry.timedOut || entry.driverErrored || entry.applyFailed {
		return true
	}
	return entry.readyMarked && entry.registrationID != 0
}

func addPendingRapidEntryPollable(
	model addPendingRapidModel,
	correlationID int64,
	entry addPendingRapidEntry,
) bool {
	if !entry.present {
		return false
	}
	return rapidPendingPollCompletes(
		model.receivedCorrelationID,
		correlationID,
		addPendingRapidEntryResolved(entry),
	)
}

func addPendingRapidPollableIDs(model addPendingRapidModel) []int64 {
	ids := make([]int64, 0, len(model.entries))
	for correlationID, entry := range model.entries {
		if addPendingRapidEntryPollable(model, correlationID, entry) {
			ids = append(ids, correlationID)
		}
	}
	return ids
}

func drawAddPendingRapidPollable(
	rt *rapid.T,
	model addPendingRapidModel,
	label string,
) (int64, bool) {
	rt.Helper()

	ids := addPendingRapidPollableIDs(model)
	if len(ids) == 0 {
		return 0, false
	}
	return drawRapidInt64Key(rt, ids, label), true
}

func drawAddPendingRapidAbsentPresentCorrelationID(
	rt *rapid.T,
	model addPendingRapidModel,
	label string,
) int64 {
	rt.Helper()

	return drawRapidAbsentCorrelationID(rt, label, func(id int64) bool {
		entry, exists := model.entries[id]
		return exists && entry.present
	})
}

type rapidPublicationPendingSpec struct {
	kind                conductor.PendingKind
	streamID            int32
	logFileName         string
	goldenCorrelationID int64
	seedLog             func(testing.TB, *conductor.Conductor)
	eventFor            func(int64) conductor.PublicationBuffersReady
	deliverReady        func(*conductor.Conductor, conductor.PublicationBuffersReady)
	isRealHandle        func(conductor.PublicationHandle) bool
}

var (
	rapidAddPublicationSpec = rapidPublicationPendingSpec{
		kind:                conductor.PendingKindAddPublication,
		streamID:            goldenPublicationBuffersReadyEvent.StreamID,
		logFileName:         goldenPublicationReadyLogFileName,
		goldenCorrelationID: goldenPublicationReadyCorrelationID,
		seedLog:             seedGoldenPublicationLogBuffers,
		eventFor:            goldenPublicationBuffersReadyWithCorrelation,
		deliverReady: func(
			conductorInstance *conductor.Conductor,
			event conductor.PublicationBuffersReady,
		) {
			conductorInstance.PublicationReadyForTest(event)
		},
		isRealHandle: func(pub conductor.PublicationHandle) bool {
			_, ok := pub.(*aeron.Publication)
			return ok
		},
	}
	rapidAddExclusivePublicationSpec = rapidPublicationPendingSpec{
		kind:                conductor.PendingKindAddExclusivePublication,
		streamID:            goldenExclusivePublicationBuffersReadyEvent.StreamID,
		logFileName:         goldenExclusivePublicationReadyLogFileName,
		goldenCorrelationID: goldenExclusivePublicationReadyCorrelationID,
		seedLog:             seedGoldenExclusivePublicationLogBuffers,
		eventFor:            goldenExclusivePublicationBuffersReadyWithCorrelation,
		deliverReady: func(
			conductorInstance *conductor.Conductor,
			event conductor.PublicationBuffersReady,
		) {
			conductorInstance.ExclusivePublicationReadyForTest(event)
		},
		isRealHandle: func(pub conductor.PublicationHandle) bool {
			_, ok := pub.(*aeron.ExclusivePublication)
			return ok
		},
	}
)

func ensureRapidPublicationLogBuffersSeeded(
	t *testing.T,
	conductorInstance *conductor.Conductor,
	spec rapidPublicationPendingSpec,
) {
	t.Helper()

	if conductor.LogBuffersRegistryRefCountForTest(
		conductor.LogBuffersRegistryForTest(conductorInstance),
		spec.logFileName,
	) == 0 {
		spec.seedLog(t, conductorInstance)
	}
}

func registerAddPendingPublicationRapidEntry(
	rt *rapid.T,
	conductorInstance *conductor.Conductor,
	nowNs int64,
	seen map[int64]struct{},
	step int,
	spec rapidPublicationPendingSpec,
	namePrefix string,
) (int64, addPendingRapidEntry) {
	rt.Helper()

	correlationID := drawPendingRapidUniqueCorrelationID(
		rt,
		rapidMapContains(seen),
		fmt.Sprintf("%sCorrelationID%d", namePrefix, step),
	)
	seen[correlationID] = struct{}{}
	deadlineNs := nowNs + pendingRapidDeadlineOffset(
		rt,
		fmt.Sprintf("%sDeadline%d", namePrefix, step),
	)
	require.NoError(
		rt,
		conductorInstance.RegisterPendingAddWithDeadline(
			spec.kind,
			correlationID,
			spec.streamID,
			pendingTestChannel,
			deadlineNs,
		),
	)
	return correlationID, addPendingRapidEntry{
		kind:       spec.kind,
		present:    true,
		deadlineNs: deadlineNs,
	}
}

func registerAddPendingRapidPublication(
	rt *rapid.T,
	conductorInstance *conductor.Conductor,
	model *addPendingRapidModel,
	seen map[int64]struct{},
	step int,
	spec rapidPublicationPendingSpec,
	namePrefix string,
) {
	rt.Helper()

	correlationID, entry := registerAddPendingPublicationRapidEntry(
		rt,
		conductorInstance,
		model.nowNs,
		seen,
		step,
		spec,
		namePrefix,
	)
	model.entries[correlationID] = entry
}

func registerAddPendingBindRetirePublication(
	rt *rapid.T,
	conductorInstance *conductor.Conductor,
	model *addPendingBindRetireRapidModel,
	seen map[int64]struct{},
	step int,
	spec rapidPublicationPendingSpec,
	namePrefix string,
) {
	rt.Helper()

	correlationID, entry := registerAddPendingPublicationRapidEntry(
		rt,
		conductorInstance,
		model.nowNs,
		seen,
		step,
		spec,
		namePrefix,
	)
	model.entries[correlationID] = addPendingBindRetireRapidEntry{
		addPendingRapidEntry: entry,
	}
}

func completeAddPendingRapidPublicationReady(
	t *testing.T,
	conductorInstance *conductor.Conductor,
	model *addPendingRapidModel,
	spec rapidPublicationPendingSpec,
	correlationID int64,
) {
	t.Helper()

	ensureRapidPublicationLogBuffersSeeded(t, conductorInstance, spec)
	event := spec.eventFor(correlationID)
	event.RegistrationID = correlationID + 1
	spec.deliverReady(conductorInstance, event)
	if !receivedCorrelationMatches(conductorInstance, correlationID) {
		return
	}
	applyAddPendingRapidReadyMark(model, correlationID, event.RegistrationID, nil)
}

func applyAddPendingRapidReadyMark(
	model *addPendingRapidModel,
	correlationID int64,
	registrationID int64,
	mutate func(*addPendingRapidEntry),
) {
	entry := model.entries[correlationID]
	entry.readyMarked = true
	entry.registrationID = registrationID
	if mutate != nil {
		mutate(&entry)
	}
	model.entries[correlationID] = entry
	model.receivedCorrelationID = correlationID
}

func completeAddPendingBindRetirePublicationReady(
	t *testing.T,
	conductorInstance *conductor.Conductor,
	lifecycle *addPendingBindRetireRapidLifecycle,
	model *addPendingBindRetireRapidModel,
	spec rapidPublicationPendingSpec,
	correlationID int64,
) {
	t.Helper()

	event := spec.eventFor(correlationID)
	event.RegistrationID = correlationID + 1
	if lifecycle.useRealPublication {
		seedPublicationCountersForTest(t, conductorInstance, event)
	}
	ensureRapidPublicationLogBuffersSeeded(t, conductorInstance, spec)
	spec.deliverReady(conductorInstance, event)
	if !receivedCorrelationMatches(conductorInstance, correlationID) {
		return
	}
	entry := model.entries[correlationID]
	entry.readyMarked = true
	entry.registrationID = event.RegistrationID
	if pub, pubOK := conductorInstance.PublicationByRegID(event.RegistrationID); pubOK {
		if spec.isRealHandle(pub) {
			entry.realHandle = true
			entry.handle = pub
		}
	}
	model.entries[correlationID] = entry
	model.receivedCorrelationID = correlationID
}

func applyAddPendingRapidCancel(model *addPendingRapidModel, correlationID int64) {
	entry, ok := model.entries[correlationID]
	if !ok || !entry.present {
		return
	}
	entry.present = false
	model.entries[correlationID] = entry
	clearReceivedCorrelationIDIf(&model.receivedCorrelationID, correlationID)
}

func applyAddPendingRapidClear(model *addPendingRapidModel) {
	for correlationID, entry := range model.entries {
		if !entry.present {
			continue
		}
		entry.present = false
		model.entries[correlationID] = entry
	}
	model.receivedCorrelationID = conductor.NullCorrelationID
	model.activeCorrelationID = conductor.NullCorrelationID
}

func applyAddPendingRapidDriverError(model *addPendingRapidModel, correlationID int64) {
	entry, ok := model.entries[correlationID]
	if !ok || !entry.present || entry.readyMarked {
		return
	}
	entry.driverErrored = true
	model.entries[correlationID] = entry
	model.receivedCorrelationID = correlationID
}

func syncAddPendingRapidAfterDoWork(
	conductorInstance *conductor.Conductor,
	model *addPendingRapidModel,
) {
	for correlationID, entry := range model.entries {
		if !entry.present {
			continue
		}
		got, ok := conductorInstance.PendingResult(correlationID)
		if !ok {
			entry.present = false
			model.entries[correlationID] = entry
			continue
		}
		entry.deadlineNs = got.DeadlineNs
		if errors.Is(got.Err, conductor.ErrAwaitTimeout) &&
			!entry.timedOut &&
			!entry.driverErrored {
			entry.timedOut = true
			model.entries[correlationID] = entry
		}
	}
	model.receivedCorrelationID = conductorInstance.ReceivedCorrelationID()
}

func applyAddPendingRapidPoll(model *addPendingRapidModel, correlationID int64) {
	entry, ok := model.entries[correlationID]
	if !ok || !entry.present {
		return
	}
	marked := rapidPendingPollMarked(model.receivedCorrelationID, correlationID)
	entry.present = false
	model.entries[correlationID] = entry
	if marked {
		clearReceivedCorrelationIDIf(&model.receivedCorrelationID, correlationID)
	}
}

func assertAddPendingRapidCorrelationInvariants(
	rt *rapid.T,
	conductorInstance *conductor.Conductor,
	model addPendingRapidModel,
) {
	rt.Helper()

	require.Equal(rt, model.receivedCorrelationID, conductorInstance.ReceivedCorrelationID())
	require.Equal(rt, model.activeCorrelationID, conductorInstance.ActiveCorrelationID())
	for correlationID, entry := range model.entries {
		_, ok := conductorInstance.PendingResult(correlationID)
		require.Equal(rt, entry.present, ok, "correlation %d pending membership", correlationID)
	}
	if model.receivedCorrelationID != conductor.NullCorrelationID {
		_, ok := model.entries[model.receivedCorrelationID]
		require.True(rt, ok)
		require.True(rt, model.entries[model.receivedCorrelationID].present)
	}
}

func assertAddPendingRapidInvariants(
	rt *rapid.T,
	conductorInstance *conductor.Conductor,
	lifecycle *recordingResourceLifecycle,
	model addPendingRapidModel,
) {
	rt.Helper()

	assertAddPendingRapidCorrelationInvariants(rt, conductorInstance, model)
	require.Equal(
		rt,
		conductorInstance.SubscriptionCountForTest(),
		lifecycle.newPreSubscriptionCalls-lifecycle.retireSubscriptionCalls,
	)
}

type addPendingRapidEligibility struct {
	completablePublication          bool
	completableExclusivePublication bool
	completableSubscription         bool
	anyPresent                      bool
	pollable                        bool
}

func scanAddPendingRapidEligibility(model addPendingRapidModel) addPendingRapidEligibility {
	var eligibility addPendingRapidEligibility
	for correlationID, entry := range model.entries {
		if !entry.present {
			continue
		}
		eligibility.anyPresent = true
		if addPendingRapidEntryPollable(model, correlationID, entry) {
			eligibility.pollable = true
		}
		if !addPendingRapidEntryCompletable(entry) {
			continue
		}
		switch entry.kind {
		case conductor.PendingKindAddPublication:
			eligibility.completablePublication = true
		case conductor.PendingKindAddExclusivePublication:
			eligibility.completableExclusivePublication = true
		case conductor.PendingKindAddSubscription:
			eligibility.completableSubscription = true
		}
	}
	return eligibility
}

func runAddPendingRapidStep(
	t *testing.T,
	rt *rapid.T,
	conductorInstance *conductor.Conductor,
	lifecycle *recordingResourceLifecycle,
	nanoNs *int64,
	model *addPendingRapidModel,
	seen map[int64]struct{},
	step int,
) {
	rt.Helper()

	eligibility := scanAddPendingRapidEligibility(*model)
	ops := []addPendingRapidOpKind{
		addPendingRapidOpRegisterPub,
		addPendingRapidOpRegisterExclusivePub,
		addPendingRapidOpRegisterSub,
	}
	if eligibility.completablePublication {
		ops = append(ops, addPendingRapidOpCompletePubReady)
	}
	if eligibility.completableExclusivePublication {
		ops = append(ops, addPendingRapidOpCompleteExclusivePubReady)
	}
	if eligibility.completableSubscription {
		ops = append(ops, addPendingRapidOpCompleteSubReady)
	}
	if eligibility.anyPresent {
		ops = append(
			ops,
			addPendingRapidOpSetActive,
			addPendingRapidOpExpire,
			addPendingRapidOpClear,
			addPendingRapidOpCancel,
			addPendingRapidOpDriverError,
		)
	}
	if eligibility.pollable {
		ops = append(ops, addPendingRapidOpPoll)
	}

	switch rapid.SampledFrom(ops).Draw(rt, fmt.Sprintf("op%d", step)) {
	case addPendingRapidOpRegisterPub:
		registerAddPendingRapidPublication(
			rt,
			conductorInstance,
			model,
			seen,
			step,
			rapidAddPublicationSpec,
			"pub",
		)

	case addPendingRapidOpRegisterExclusivePub:
		registerAddPendingRapidPublication(
			rt,
			conductorInstance,
			model,
			seen,
			step,
			rapidAddExclusivePublicationSpec,
			"exclusivePub",
		)

	case addPendingRapidOpRegisterSub:
		correlationID := drawPendingRapidUniqueCorrelationID(
			rt,
			rapidMapContains(seen),
			fmt.Sprintf("subCorrelationID%d", step),
		)
		seen[correlationID] = struct{}{}
		deadlineNs := model.nowNs + pendingRapidDeadlineOffset(rt, fmt.Sprintf("subDeadline%d", step))
		require.NoError(
			rt,
			conductorInstance.RegisterPendingAddWithDeadline(
				conductor.PendingKindAddSubscription,
				correlationID,
				pendingTestStreamID,
				pendingTestChannel,
				deadlineNs,
			),
		)
		model.entries[correlationID] = addPendingRapidEntry{
			kind:       conductor.PendingKindAddSubscription,
			present:    true,
			deadlineNs: deadlineNs,
		}
	case addPendingRapidOpCompletePubReady:
		correlationID, ok := drawAddPendingRapidCompletable(*model, conductor.PendingKindAddPublication)
		if !ok {
			return
		}
		completeAddPendingRapidPublicationReady(
			t,
			conductorInstance,
			model,
			rapidAddPublicationSpec,
			correlationID,
		)

	case addPendingRapidOpCompleteExclusivePubReady:
		correlationID, ok := drawAddPendingRapidCompletable(
			*model,
			conductor.PendingKindAddExclusivePublication,
		)
		if !ok {
			return
		}
		completeAddPendingRapidPublicationReady(
			t,
			conductorInstance,
			model,
			rapidAddExclusivePublicationSpec,
			correlationID,
		)

	case addPendingRapidOpCompleteSubReady:
		correlationID, ok := drawAddPendingRapidCompletable(*model, conductor.PendingKindAddSubscription)
		if !ok {
			return
		}
		applyFails := rapid.Bool().Draw(rt, fmt.Sprintf("applySubFail%d", step))
		if applyFails {
			lifecycle.applySubscriptionErr = errRapidApplySubscriptionReady
		} else {
			lifecycle.applySubscriptionErr = nil
		}
		event := goldenSubscriptionReadyWithCorrelation(correlationID)
		conductorInstance.SubscriptionReadyForTest(event)
		if !receivedCorrelationMatches(conductorInstance, correlationID) {
			return
		}
		applyAddPendingRapidReadyMark(
			model,
			correlationID,
			correlationID,
			func(entry *addPendingRapidEntry) {
				entry.applyFailed = applyFails
			},
		)

	case addPendingRapidOpSetActive:
		if len(model.entries) == 0 {
			model.activeCorrelationID = conductor.NullCorrelationID
			conductorInstance.SetActiveCorrelationID(conductor.NullCorrelationID)
			return
		}
		if rapid.Bool().Draw(rt, fmt.Sprintf("activeNull%d", step)) {
			model.activeCorrelationID = conductor.NullCorrelationID
			conductorInstance.SetActiveCorrelationID(conductor.NullCorrelationID)
			return
		}
		correlationID, ok := drawAddPendingRapidPresent(*model)
		if !ok {
			return
		}
		model.activeCorrelationID = correlationID
		conductorInstance.SetActiveCorrelationID(correlationID)

	case addPendingRapidOpCancel:
		if rapid.Bool().Draw(rt, fmt.Sprintf("cancelAbsent%d", step)) {
			correlationID := drawAddPendingRapidAbsentPresentCorrelationID(
				rt,
				*model,
				fmt.Sprintf("cancelAbsentCorrelationID%d", step),
			)
			conductorInstance.CancelPending(correlationID)
			return
		}
		correlationID, ok := drawAddPendingRapidPresent(*model)
		if !ok {
			return
		}
		conductorInstance.CancelPending(correlationID)
		applyAddPendingRapidCancel(model, correlationID)

	case addPendingRapidOpClear:
		conductorInstance.ClearPending()
		applyAddPendingRapidClear(model)

	case addPendingRapidOpDriverError:
		if rapid.Bool().Draw(rt, fmt.Sprintf("driverErrorAbsent%d", step)) {
			correlationID := drawAddPendingRapidAbsentPresentCorrelationID(
				rt,
				*model,
				fmt.Sprintf("driverErrorAbsentCorrelationID%d", step),
			)
			ok := conductorInstance.DriverError(rapidDriverRejectedResponse(correlationID))
			require.False(rt, ok)
			return
		}
		correlationID, ok := drawAddPendingRapidPresent(*model)
		if !ok {
			return
		}
		entry := model.entries[correlationID]
		if entry.readyMarked || entry.driverErrored {
			return
		}
		ok = conductorInstance.DriverError(rapidDriverRejectedResponse(correlationID))
		require.True(rt, ok)
		applyAddPendingRapidDriverError(model, correlationID)

	case addPendingRapidOpExpire:
		model.nowNs += rapid.Int64Range(0, 40_000_000).Draw(rt, fmt.Sprintf("advanceNs%d", step))
		*nanoNs = model.nowNs
		_ = conductorInstance.DoWork()
		syncAddPendingRapidAfterDoWork(conductorInstance, model)

	case addPendingRapidOpPoll:
		correlationID, ok := drawAddPendingRapidPollable(
			rt,
			*model,
			fmt.Sprintf("pollCorrelationID%d", step),
		)
		if !ok {
			return
		}
		entry := model.entries[correlationID]
		if got, ok := conductorInstance.PendingResult(correlationID); ok {
			entry.applyFailed = errors.Is(got.Err, errRapidApplySubscriptionReady)
			var driverErr *conductor.DriverRegistrationError
			entry.driverErrored = errors.As(got.Err, &driverErr)
			entry.timedOut = errors.Is(got.Err, conductor.ErrAwaitTimeout) &&
				!entry.applyFailed &&
				!entry.driverErrored
		}
		done, _, err := conductorInstance.PollPending(correlationID)
		require.True(rt, done)
		switch {
		case entry.driverErrored:
			var driverErr *conductor.DriverRegistrationError
			require.ErrorAs(rt, err, &driverErr)
			require.Equal(rt, correlationID, driverErr.CorrelationID)
		case entry.applyFailed:
			require.ErrorIs(rt, err, errRapidApplySubscriptionReady)
		case entry.timedOut:
			require.ErrorIs(rt, err, conductor.ErrAwaitTimeout)
			require.EqualError(rt, err, "conductor: await response timeout")
		case entry.readyMarked:
			require.NoError(rt, err)
		default:
			require.ErrorIs(rt, err, conductor.ErrAwaitTimeout)
			require.EqualError(rt, err, "conductor: await response timeout")
		}
		applyAddPendingRapidPoll(model, correlationID)
	}
}

func TestConductor_addPending_teardown_rapid(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(rt *rapid.T) {
		nanoNs := pendingRapidBaseNowNs
		conductorInstance, lifecycle := newConductorWithResourceLifecycleMutableNanoForTest(t, 0, &nanoNs)
		conductorInstance.SeedServiceTimestamps(nanoNs)
		model := addPendingRapidModel{
			entries:               make(map[int64]addPendingRapidEntry),
			receivedCorrelationID: conductor.NullCorrelationID,
			activeCorrelationID:   conductor.NullCorrelationID,
			nowNs:                 pendingRapidBaseNowNs,
		}
		seen := make(map[int64]struct{})
		steps := rapid.IntRange(1, 16).Draw(rt, "steps")
		for step := range steps {
			runAddPendingRapidStep(t, rt, conductorInstance, lifecycle, &nanoNs, &model, seen, step)
			assertAddPendingRapidInvariants(rt, conductorInstance, lifecycle, model)
		}
	})
}

type addPendingBindRetireRapidLifecycle struct {
	realCreateMarkRecordingLifecycle
	useRealPublication  bool
	useRealSubscription bool
}

func (lifecycle *addPendingBindRetireRapidLifecycle) clearFailureMode() {
	lifecycle.useRealPublication = false
	lifecycle.useRealSubscription = false
	lifecycle.bindPublicationErr = nil
	lifecycle.bindSubscriptionErr = nil
	lifecycle.retirePublicationErr = nil
	lifecycle.retireSubscriptionErr = nil
}

type addPendingBindRetireFailureMode int

const (
	addPendingBindRetireFailureNone addPendingBindRetireFailureMode = iota
	addPendingBindRetireFailureBindPublication
	addPendingBindRetireFailureBindSubscription
	addPendingBindRetireFailureRetirePublication
	addPendingBindRetireFailureRetireSubscription
)

func (lifecycle *addPendingBindRetireRapidLifecycle) applyFailureMode(
	mode addPendingBindRetireFailureMode,
) {
	lifecycle.clearFailureMode()
	switch mode {
	case addPendingBindRetireFailureBindPublication:
		lifecycle.bindPublicationErr = errStubBindPublication
	case addPendingBindRetireFailureBindSubscription:
		lifecycle.bindSubscriptionErr = errStubBindSubscription
	case addPendingBindRetireFailureRetirePublication:
		lifecycle.useRealPublication = true
		lifecycle.retirePublicationErr = errRetirePublication
	case addPendingBindRetireFailureRetireSubscription:
		lifecycle.useRealSubscription = true
		lifecycle.retireSubscriptionErr = errRetireSubscription
	}
}

func (lifecycle *addPendingBindRetireRapidLifecycle) NewPublication(
	event conductor.PublicationBuffersReady,
	logBuffers *logbuffer.LogBuffers,
	countersReader *cnc.CountersReader,
	channel string,
) (conductor.PublicationHandle, error) {
	if lifecycle.useRealPublication {
		lifecycle.newPublicationCalls++
		if lifecycle.newPublicationErr != nil {
			return nil, lifecycle.newPublicationErr
		}
		return lifecycle.realCreateMarkRecordingLifecycle.NewPublication(
			event,
			logBuffers,
			countersReader,
			channel,
		)
	}
	return lifecycle.recordingResourceLifecycle.NewPublication(
		event,
		logBuffers,
		countersReader,
		channel,
	)
}

func (lifecycle *addPendingBindRetireRapidLifecycle) NewExclusivePublication(
	event conductor.PublicationBuffersReady,
	logBuffers *logbuffer.LogBuffers,
	countersReader *cnc.CountersReader,
	channel string,
) (conductor.PublicationHandle, error) {
	if lifecycle.useRealPublication {
		lifecycle.newExclusivePublicationCalls++
		if lifecycle.newExclusivePublicationErr != nil {
			return nil, lifecycle.newExclusivePublicationErr
		}
		return lifecycle.realCreateMarkRecordingLifecycle.NewExclusivePublication(
			event,
			logBuffers,
			countersReader,
			channel,
		)
	}
	return lifecycle.recordingResourceLifecycle.NewExclusivePublication(
		event,
		logBuffers,
		countersReader,
		channel,
	)
}

func (lifecycle *addPendingBindRetireRapidLifecycle) NewPreSubscription(
	registrationID int64,
	streamID int32,
	channel string,
) conductor.SubscriptionHandle {
	if lifecycle.useRealSubscription {
		lifecycle.newPreSubscriptionCalls++
		return lifecycle.realCreateMarkRecordingLifecycle.NewPreSubscription(
			registrationID,
			streamID,
			channel,
		)
	}
	return lifecycle.recordingResourceLifecycle.NewPreSubscription(
		registrationID,
		streamID,
		channel,
	)
}

func (lifecycle *addPendingBindRetireRapidLifecycle) ApplySubscriptionReady(
	sub conductor.SubscriptionHandle,
	event conductor.SubscriptionReady,
) error {
	_, isRealSub := sub.(*aeron.Subscription)
	if isRealSub {
		lifecycle.applySubscriptionCalls++
		if lifecycle.applySubscriptionErr != nil {
			return lifecycle.applySubscriptionErr
		}
		return lifecycle.realCreateMarkRecordingLifecycle.ApplySubscriptionReady(sub, event)
	}
	return lifecycle.recordingResourceLifecycle.ApplySubscriptionReady(sub, event)
}

func (lifecycle *addPendingBindRetireRapidLifecycle) BindPublication(
	pub conductor.PublicationHandle,
	clientConductor *conductor.Conductor,
) error {
	if lifecycle.useRealPublication {
		lifecycle.bindPublicationCalls++
		if lifecycle.bindPublicationErr != nil {
			return lifecycle.bindPublicationErr
		}
		return lifecycle.realCreateMarkRecordingLifecycle.BindPublication(pub, clientConductor)
	}
	return lifecycle.recordingResourceLifecycle.BindPublication(pub, clientConductor)
}

func (lifecycle *addPendingBindRetireRapidLifecycle) BindSubscription(
	sub conductor.SubscriptionHandle,
	clientConductor *conductor.Conductor,
) error {
	if lifecycle.useRealSubscription {
		lifecycle.bindSubscriptionCalls++
		if lifecycle.bindSubscriptionErr != nil {
			return lifecycle.bindSubscriptionErr
		}
		return lifecycle.realCreateMarkRecordingLifecycle.BindSubscription(sub, clientConductor)
	}
	return lifecycle.recordingResourceLifecycle.BindSubscription(sub, clientConductor)
}

func (lifecycle *addPendingBindRetireRapidLifecycle) MarkPublicationClosed(
	pub conductor.PublicationHandle,
) {
	switch pub.(type) {
	case *aeron.Publication, *aeron.ExclusivePublication:
		lifecycle.realCreateMarkRecordingLifecycle.MarkPublicationClosed(pub)
	}
}

func (lifecycle *addPendingBindRetireRapidLifecycle) MarkSubscriptionClosed(
	sub conductor.SubscriptionHandle,
) {
	if _, isRealSub := sub.(*aeron.Subscription); isRealSub {
		lifecycle.realCreateMarkRecordingLifecycle.MarkSubscriptionClosed(sub)
	}
}

type addPendingBindRetireRapidEntry struct {
	addPendingRapidEntry
	realHandle bool
	handle     any
}

type addPendingBindRetireRapidModel struct {
	entries               map[int64]addPendingBindRetireRapidEntry
	receivedCorrelationID int64
	activeCorrelationID   int64
	nowNs                 int64
}

func (model addPendingBindRetireRapidModel) baseModel() addPendingRapidModel {
	entries := make(map[int64]addPendingRapidEntry, len(model.entries))
	for correlationID, entry := range model.entries {
		entries[correlationID] = entry.addPendingRapidEntry
	}
	return addPendingRapidModel{
		entries:               entries,
		receivedCorrelationID: model.receivedCorrelationID,
		activeCorrelationID:   model.activeCorrelationID,
		nowNs:                 model.nowNs,
	}
}

func drawAddPendingBindRetireCompletable(
	model addPendingBindRetireRapidModel,
	kind conductor.PendingKind,
) (int64, bool) {
	return drawAddPendingRapidCompletable(model.baseModel(), kind)
}

func drawAddPendingBindRetirePresent(
	model addPendingBindRetireRapidModel,
) (int64, bool) {
	return drawAddPendingRapidPresent(model.baseModel())
}

func applyAddPendingBindRetireCancel(
	model *addPendingBindRetireRapidModel,
	correlationID int64,
) {
	entry, ok := model.entries[correlationID]
	if !ok || !entry.present {
		return
	}
	entry.present = false
	model.entries[correlationID] = entry
	clearReceivedCorrelationIDIf(&model.receivedCorrelationID, correlationID)
}

func applyAddPendingBindRetireClear(model *addPendingBindRetireRapidModel) {
	base := model.baseModel()
	applyAddPendingRapidClear(&base)
	model.receivedCorrelationID = base.receivedCorrelationID
	model.activeCorrelationID = base.activeCorrelationID
	for correlationID, entry := range model.entries {
		if baseEntry, ok := base.entries[correlationID]; ok {
			entry.present = baseEntry.present
			model.entries[correlationID] = entry
		}
	}
}

func applyAddPendingBindRetirePoll(
	model *addPendingBindRetireRapidModel,
	correlationID int64,
) {
	entry, ok := model.entries[correlationID]
	if !ok || !entry.present {
		return
	}
	marked := rapidPendingPollMarked(model.receivedCorrelationID, correlationID)
	entry.present = false
	model.entries[correlationID] = entry
	if marked {
		clearReceivedCorrelationIDIf(&model.receivedCorrelationID, correlationID)
	}
}

func syncAddPendingBindRetireAfterDoWork(
	conductorInstance *conductor.Conductor,
	model *addPendingBindRetireRapidModel,
) {
	base := model.baseModel()
	syncAddPendingRapidAfterDoWork(conductorInstance, &base)
	model.receivedCorrelationID = base.receivedCorrelationID
	for correlationID, entry := range model.entries {
		if baseEntry, ok := base.entries[correlationID]; ok {
			entry.present = baseEntry.present
			entry.timedOut = baseEntry.timedOut
			model.entries[correlationID] = entry
		}
	}
}

func scanAddPendingBindRetireEligibility(
	model addPendingBindRetireRapidModel,
) addPendingRapidEligibility {
	return scanAddPendingRapidEligibility(model.baseModel())
}

func assertAddPendingBindRetireHandleClosed(
	rt *rapid.T,
	entry addPendingBindRetireRapidEntry,
) {
	rt.Helper()
	if !entry.realHandle || entry.handle == nil {
		return
	}
	switch entry.kind {
	case conductor.PendingKindAddPublication:
		require.True(rt, entry.handle.(*aeron.Publication).IsClosed())
	case conductor.PendingKindAddExclusivePublication:
		require.True(rt, entry.handle.(*aeron.ExclusivePublication).IsClosed())
	case conductor.PendingKindAddSubscription:
		require.True(rt, entry.handle.(*aeron.Subscription).IsClosed())
	}
}

func assertAddPendingBindRetireRollback(
	rt *rapid.T,
	conductorInstance *conductor.Conductor,
	mode addPendingBindRetireFailureMode,
	correlationID int64,
	entry addPendingBindRetireRapidEntry,
	gotErrors *[]error,
	errorsBefore int,
) {
	rt.Helper()
	if !entry.realHandle {
		return
	}
	switch mode {
	case addPendingBindRetireFailureRetirePublication:
		if !conductor.AddPublicationPendingKindForTest(entry.kind) {
			return
		}
		require.Greater(rt, len(*gotErrors), errorsBefore)
		require.ErrorIs(rt, (*gotErrors)[len(*gotErrors)-1], errRetirePublication)
		assertAddPendingBindRetireHandleClosed(rt, entry)
		_, pubOK := conductorInstance.PublicationByRegID(entry.registrationID)
		require.False(rt, pubOK)
	case addPendingBindRetireFailureRetireSubscription:
		if entry.kind != conductor.PendingKindAddSubscription {
			return
		}
		require.Greater(rt, len(*gotErrors), errorsBefore)
		require.ErrorIs(rt, (*gotErrors)[len(*gotErrors)-1], errRetireSubscription)
		assertAddPendingBindRetireHandleClosed(rt, entry)
		_, subOK := conductorInstance.SubscriptionByRegID(correlationID)
		require.False(rt, subOK)
	}
}

func snapshotBindRetireRollbackTargets(
	model addPendingBindRetireRapidModel,
	include func(addPendingBindRetireRapidEntry) bool,
) map[int64]addPendingBindRetireRapidEntry {
	rollbackTargets := make(map[int64]addPendingBindRetireRapidEntry)
	for correlationID, entry := range model.entries {
		if include(entry) {
			rollbackTargets[correlationID] = entry
		}
	}
	return rollbackTargets
}

func assertAddPendingBindRetireRapidInvariants(
	rt *rapid.T,
	conductorInstance *conductor.Conductor,
	lifecycle *addPendingBindRetireRapidLifecycle,
	model addPendingBindRetireRapidModel,
) {
	rt.Helper()

	assertAddPendingRapidCorrelationInvariants(rt, conductorInstance, model.baseModel())
	require.Equal(
		rt,
		conductorInstance.SubscriptionCountForTest(),
		lifecycle.newPreSubscriptionCalls-lifecycle.retireSubscriptionCalls,
	)
}

func runAddPendingBindRetireFailureRapidStep(
	t *testing.T,
	rt *rapid.T,
	conductorInstance *conductor.Conductor,
	lifecycle *addPendingBindRetireRapidLifecycle,
	gotErrors *[]error,
	nanoNs *int64,
	model *addPendingBindRetireRapidModel,
	seen map[int64]struct{},
	step int,
) {
	rt.Helper()

	mode := addPendingBindRetireFailureMode(
		rapid.IntRange(0, 4).Draw(rt, fmt.Sprintf("failureMode%d", step)),
	)
	lifecycle.applyFailureMode(mode)

	eligibility := scanAddPendingBindRetireEligibility(*model)
	ops := []addPendingRapidOpKind{
		addPendingRapidOpRegisterPub,
		addPendingRapidOpRegisterExclusivePub,
		addPendingRapidOpRegisterSub,
	}
	if eligibility.completablePublication {
		ops = append(ops, addPendingRapidOpCompletePubReady)
	}
	if eligibility.completableExclusivePublication {
		ops = append(ops, addPendingRapidOpCompleteExclusivePubReady)
	}
	if eligibility.completableSubscription {
		ops = append(ops, addPendingRapidOpCompleteSubReady)
	}
	if eligibility.anyPresent {
		ops = append(ops, addPendingRapidOpCancel, addPendingRapidOpClear, addPendingRapidOpExpire)
	}
	if eligibility.pollable {
		ops = append(ops, addPendingRapidOpPoll)
	}

	switch rapid.SampledFrom(ops).Draw(rt, fmt.Sprintf("op%d", step)) {
	case addPendingRapidOpRegisterPub:
		registerAddPendingBindRetirePublication(
			rt,
			conductorInstance,
			model,
			seen,
			step,
			rapidAddPublicationSpec,
			"pub",
		)

	case addPendingRapidOpRegisterExclusivePub:
		registerAddPendingBindRetirePublication(
			rt,
			conductorInstance,
			model,
			seen,
			step,
			rapidAddExclusivePublicationSpec,
			"exclusivePub",
		)

	case addPendingRapidOpRegisterSub:
		correlationID := drawPendingRapidUniqueCorrelationID(
			rt,
			rapidMapContains(seen),
			fmt.Sprintf("subCorrelationID%d", step),
		)
		seen[correlationID] = struct{}{}
		deadlineNs := model.nowNs + pendingRapidDeadlineOffset(rt, fmt.Sprintf("subDeadline%d", step))
		subscriptionCountBefore := conductorInstance.SubscriptionCountForTest()
		err := conductorInstance.RegisterPendingAddWithDeadline(
			conductor.PendingKindAddSubscription,
			correlationID,
			pendingTestStreamID,
			pendingTestChannel,
			deadlineNs,
		)
		if mode == addPendingBindRetireFailureBindSubscription {
			require.ErrorIs(rt, err, errStubBindSubscription)
			require.Equal(rt, subscriptionCountBefore, conductorInstance.SubscriptionCountForTest())
			_, pendingOK := conductorInstance.PendingResult(correlationID)
			require.False(rt, pendingOK)
			_, subOK := conductorInstance.SubscriptionByRegID(correlationID)
			require.False(rt, subOK)
			return
		}
		require.NoError(rt, err)
		entry := addPendingBindRetireRapidEntry{
			addPendingRapidEntry: addPendingRapidEntry{
				kind:       conductor.PendingKindAddSubscription,
				present:    true,
				deadlineNs: deadlineNs,
			},
		}
		if sub, ok := conductorInstance.SubscriptionByRegID(correlationID); ok {
			if _, isRealSub := sub.(*aeron.Subscription); isRealSub {
				entry.realHandle = true
				entry.handle = sub
			}
		}
		model.entries[correlationID] = entry

	case addPendingRapidOpCompletePubReady:
		correlationID, ok := drawAddPendingBindRetireCompletable(
			*model,
			conductor.PendingKindAddPublication,
		)
		if !ok {
			return
		}
		completeAddPendingBindRetirePublicationReady(
			t,
			conductorInstance,
			lifecycle,
			model,
			rapidAddPublicationSpec,
			correlationID,
		)

	case addPendingRapidOpCompleteExclusivePubReady:
		correlationID, ok := drawAddPendingBindRetireCompletable(
			*model,
			conductor.PendingKindAddExclusivePublication,
		)
		if !ok {
			return
		}
		completeAddPendingBindRetirePublicationReady(
			t,
			conductorInstance,
			lifecycle,
			model,
			rapidAddExclusivePublicationSpec,
			correlationID,
		)

	case addPendingRapidOpCompleteSubReady:
		correlationID, ok := drawAddPendingBindRetireCompletable(
			*model,
			conductor.PendingKindAddSubscription,
		)
		if !ok {
			return
		}
		event := goldenSubscriptionReadyWithCorrelation(correlationID)
		conductorInstance.SubscriptionReadyForTest(event)
		if !receivedCorrelationMatches(conductorInstance, correlationID) {
			return
		}
		entry := model.entries[correlationID]
		entry.readyMarked = true
		entry.registrationID = correlationID
		if sub, subOK := conductorInstance.SubscriptionByRegID(correlationID); subOK {
			if _, isRealSub := sub.(*aeron.Subscription); isRealSub {
				entry.realHandle = true
				entry.handle = sub
			}
		}
		model.entries[correlationID] = entry
		model.receivedCorrelationID = correlationID

	case addPendingRapidOpCancel:
		correlationID, ok := drawAddPendingBindRetirePresent(*model)
		if !ok {
			return
		}
		entry := model.entries[correlationID]
		readyMarked := model.receivedCorrelationID == correlationID && entry.readyMarked
		errorsBefore := len(*gotErrors)
		conductorInstance.CancelPending(correlationID)
		applyAddPendingBindRetireCancel(model, correlationID)
		if !readyMarked {
			assertAddPendingBindRetireRollback(
				rt,
				conductorInstance,
				mode,
				correlationID,
				entry,
				gotErrors,
				errorsBefore,
			)
		}

	case addPendingRapidOpClear:
		rollbackTargets := snapshotBindRetireRollbackTargets(
			*model,
			func(entry addPendingBindRetireRapidEntry) bool { return entry.present },
		)
		errorsBefore := len(*gotErrors)
		conductorInstance.ClearPending()
		applyAddPendingBindRetireClear(model)
		for correlationID, entry := range rollbackTargets {
			assertAddPendingBindRetireRollback(
				rt,
				conductorInstance,
				mode,
				correlationID,
				entry,
				gotErrors,
				errorsBefore,
			)
		}

	case addPendingRapidOpExpire:
		rollbackTargets := snapshotBindRetireRollbackTargets(
			*model,
			func(entry addPendingBindRetireRapidEntry) bool {
				return entry.present && !entry.readyMarked && !entry.driverErrored
			},
		)
		errorsBefore := len(*gotErrors)
		model.nowNs += rapid.Int64Range(0, 40_000_000).Draw(rt, fmt.Sprintf("advanceNs%d", step))
		*nanoNs = model.nowNs
		_ = conductorInstance.DoWork()
		syncAddPendingBindRetireAfterDoWork(conductorInstance, model)
		for correlationID, entry := range rollbackTargets {
			current, ok := model.entries[correlationID]
			if !ok || current.present {
				continue
			}
			assertAddPendingBindRetireRollback(
				rt,
				conductorInstance,
				mode,
				correlationID,
				entry,
				gotErrors,
				errorsBefore,
			)
		}

	case addPendingRapidOpPoll:
		correlationID, ok := drawAddPendingRapidPollable(
			rt,
			model.baseModel(),
			fmt.Sprintf("pollCorrelationID%d", step),
		)
		if !ok {
			return
		}
		entry := model.entries[correlationID]
		var pendingErr error
		if got, pendingOK := conductorInstance.PendingResult(correlationID); pendingOK {
			pendingErr = got.Err
			entry.applyFailed = errors.Is(got.Err, errRapidApplySubscriptionReady)
			var driverErr *conductor.DriverRegistrationError
			entry.driverErrored = errors.As(got.Err, &driverErr)
			entry.timedOut = errors.Is(got.Err, conductor.ErrAwaitTimeout) &&
				!entry.applyFailed &&
				!entry.driverErrored
		}
		done, _, err := conductorInstance.PollPending(correlationID)
		require.True(rt, done)
		switch {
		case errors.Is(pendingErr, errStubBindPublication):
			require.ErrorIs(rt, err, errStubBindPublication)
			_, pubOK := conductorInstance.PublicationByRegID(entry.registrationID)
			require.False(rt, pubOK)
		case entry.driverErrored:
			var driverErr *conductor.DriverRegistrationError
			require.ErrorAs(rt, err, &driverErr)
			require.Equal(rt, correlationID, driverErr.CorrelationID)
		case entry.applyFailed:
			require.ErrorIs(rt, err, errRapidApplySubscriptionReady)
		case entry.timedOut:
			require.ErrorIs(rt, err, conductor.ErrAwaitTimeout)
			require.EqualError(rt, err, "conductor: await response timeout")
		case entry.readyMarked:
			require.NoError(rt, err)
		default:
			require.ErrorIs(rt, err, conductor.ErrAwaitTimeout)
			require.EqualError(rt, err, "conductor: await response timeout")
		}
		applyAddPendingBindRetirePoll(model, correlationID)
	}
}

func TestConductor_addPending_teardown_bindRetireFailure_rapid(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(rt *rapid.T) {
		nanoNs := pendingRapidBaseNowNs
		errorHandler, gotErrors := collectConductorErrors()
		lifecycle := &addPendingBindRetireRapidLifecycle{}
		config := conductorTestConfigMutableNano(t, errorHandler, 0, &nanoNs)
		config.ResourceLifecycle = lifecycle
		conductorInstance := newConductorForTest(t, config)
		conductorInstance.SeedServiceTimestamps(nanoNs)
		model := addPendingBindRetireRapidModel{
			entries:               make(map[int64]addPendingBindRetireRapidEntry),
			receivedCorrelationID: conductor.NullCorrelationID,
			activeCorrelationID:   conductor.NullCorrelationID,
			nowNs:                 pendingRapidBaseNowNs,
		}
		seen := make(map[int64]struct{})
		steps := rapid.IntRange(1, 12).Draw(rt, "steps")
		for step := range steps {
			runAddPendingBindRetireFailureRapidStep(
				t,
				rt,
				conductorInstance,
				lifecycle,
				gotErrors,
				&nanoNs,
				&model,
				seen,
				step,
			)
			assertAddPendingBindRetireRapidInvariants(rt, conductorInstance, lifecycle, model)
		}
	})
}

func TestConductor_handlePublicationReady_buildFailure_refCount_rapid(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(rt *rapid.T) {
		spec := rapidAddPublicationSpec
		if rapid.Bool().Draw(rt, "exclusive") {
			spec = rapidAddExclusivePublicationSpec
		}
		failureKind := rapid.IntRange(0, 2).Draw(rt, "failureKind")
		lifecycle := &recordingResourceLifecycle{}
		conductorInstance := newConductorWithResourceLifecycleForTest(t, lifecycle)
		registry := conductor.LogBuffersRegistryForTest(conductorInstance)

		var refCountBefore int32
		switch failureKind {
		case 0, 1:
			if failureKind == 1 {
				if spec.kind == conductor.PendingKindAddExclusivePublication {
					lifecycle.newExclusivePublicationErr = errStubPublicationBuild
				} else {
					lifecycle.newPublicationErr = errStubPublicationBuild
				}
			}
			spec.seedLog(t, conductorInstance)
			refCountBefore = conductor.LogBuffersRegistryRefCountForTest(
				registry,
				spec.logFileName,
			)
			require.Equal(rt, int32(1), refCountBefore)
		default:
			refCountBefore = conductor.LogBuffersRegistryRefCountForTest(
				registry,
				spec.logFileName,
			)
			require.Zero(rt, refCountBefore)
		}

		require.NoError(
			rt,
			registerPendingAddWithCorrelationForTest(
				conductorInstance,
				spec.kind,
				spec.goldenCorrelationID,
				spec.streamID,
			),
		)
		spec.deliverReady(
			conductorInstance,
			spec.eventFor(spec.goldenCorrelationID),
		)

		done, _, err := conductorInstance.PollPending(spec.goldenCorrelationID)
		require.True(rt, done)
		_, pendingOK := conductorInstance.PendingResult(spec.goldenCorrelationID)
		require.False(rt, pendingOK)
		require.Equal(rt, conductor.NullCorrelationID, conductorInstance.ReceivedCorrelationID())

		refCountAfter := conductor.LogBuffersRegistryRefCountForTest(
			registry,
			spec.logFileName,
		)
		switch failureKind {
		case 0:
			require.NoError(rt, err)
			require.Equal(rt, 1, conductorInstance.PublicationCountForTest())
			require.Equal(rt, int32(2), refCountAfter)
		case 1:
			require.ErrorIs(rt, err, errStubPublicationBuild)
			require.Equal(rt, 0, conductorInstance.PublicationCountForTest())
			require.Equal(rt, refCountBefore, refCountAfter)
		default:
			require.EqualError(rt, err, "conductor: failed to map log buffer")
			require.Equal(rt, 0, conductorInstance.PublicationCountForTest())
			require.Equal(rt, refCountBefore, refCountAfter)
		}
	})
}

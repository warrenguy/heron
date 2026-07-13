//go:build rapid

package archive

import (
	"testing"

	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"

	"github.com/warrenguy/heron/archive/codecs"
	"github.com/warrenguy/heron/archive/codecs/archivewire"
)

func descriptorRapidResponseWire(
	t *testing.T,
	correlationID int64,
	code archivewire.ControlResponseCode,
) []byte {
	t.Helper()
	return encodeControlResponseWire(t, codecs.ControlResponseFields{
		ControlSessionID: controlResponseTestControlSessionID,
		CorrelationID:    correlationID,
		Code:             code,
		Version:          520,
	})
}

type descriptorEventKind int

const (
	descriptorEventResponseOK descriptorEventKind = iota
	descriptorEventResponseError
	descriptorEventDescriptor
)

type descriptorPollerModel struct {
	await       int64
	complete    bool
	hasResponse bool
	responseOK  bool
}

func (m *descriptorPollerModel) applyResponse(correlationID int64, ok bool) {
	if m.complete || correlationID != m.await {
		return
	}
	m.hasResponse = true
	m.responseOK = ok
	if !ok {
		m.complete = true
	}
}

func (m *descriptorPollerModel) applyDescriptor(correlationID int64) {
	if m.complete || correlationID != m.await {
		return
	}
	// Matching descriptor always completes, with or without a preceding OK response.
	m.complete = true
}

// TestRecordingDescriptorPoller_completionModel_rapid checks the descriptor poller
// completion state machine against a reference model over random event sequences: OK and
// error responses, and matching and foreign descriptors, applied through the demux
// dispatch functions.
func TestRecordingDescriptorPoller_completionModel_rapid(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(rt *rapid.T) {
		const await int64 = 4242
		const foreign int64 = 9999

		poller := NewRecordingDescriptorPoller()
		poller.ResetAwait(await)
		model := descriptorPollerModel{await: await}

		eventCount := rapid.IntRange(1, 12).Draw(rt, "eventCount")
		for i := 0; i < eventCount; i++ {
			kind := descriptorEventKind(rapid.IntRange(0, 2).Draw(rt, "kind"))
			correlationID := rapid.SampledFrom([]int64{await, foreign}).Draw(rt, "correlationID")

			switch kind {
			case descriptorEventResponseOK:
				wire := descriptorRapidResponseWire(
					t,
					correlationID,
					archivewire.ControlResponseCode_OK,
				)
				handleDescriptorControlResponseFragment(poller, wire)
				model.applyResponse(correlationID, true)
			case descriptorEventResponseError:
				wire := descriptorRapidResponseWire(
					t,
					correlationID,
					archivewire.ControlResponseCode_ERROR,
				)
				handleDescriptorControlResponseFragment(poller, wire)
				model.applyResponse(correlationID, false)
			case descriptorEventDescriptor:
				handleRecordingDescriptorFragment(poller, encodeRecordingDescriptorWire(
					t,
					recordingDescriptorTestFields(correlationID),
				))
				model.applyDescriptor(correlationID)
			}

			require.Equal(rt, model.complete, poller.IsPollComplete())

			fields, ok := poller.ControlResponse()
			require.Equal(rt, model.hasResponse, ok)
			if !ok {
				continue
			}
			require.Equal(rt, model.await, fields.CorrelationID)
			err := MapControlResponse(fields)
			require.Equal(rt, !model.responseOK, err != nil)
		}
	})
}

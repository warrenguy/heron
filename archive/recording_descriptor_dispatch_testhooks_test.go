//go:build testhooks

package archive

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestPollControlEvents_fragmentedDescriptorCapture proves the descriptor dispatch
// runs on a reassembled buffer: a RecordingDescriptor split across two DATA frames is
// rebuilt by the demux assembler and captured with its var-ascii payload intact.
func TestPollControlEvents_fragmentedDescriptorCapture(t *testing.T) {
	sub, termBuffer := setupControlResponsePollFixtures(t)
	poller := NewRecordingDescriptorPoller()
	poller.ResetAwait(recordingDescriptorTestCorrelationID)
	assembler := NewFragmentAssembler(nil)

	descriptorWire := encodeRecordingDescriptorWire(
		t,
		recordingDescriptorTestFields(recordingDescriptorTestCorrelationID),
	)
	writeFragmentedWireToTerm(t, termBuffer, descriptorWire)

	// Two wire frames (BEGIN + END) reassemble into one descriptor delivery.
	n := PollControlEvents(
		sub,
		DefaultControlResponseFragmentLimit,
		assembler,
		ControlPollTargets{DescriptorPoller: poller},
	)
	require.Equal(t, 2, n)

	got := poller.Descriptor()
	require.Equal(t, int64(7001), got.RecordingID)
	require.Equal(t, "aeron:ipc?alias=stripped", string(got.StrippedChannel))
	require.Equal(t, "aeron:ipc?alias=original", string(got.OriginalChannel))
	require.Equal(t, "source-identity", string(got.SourceIdentity))
	require.True(t, poller.IsPollComplete())
}

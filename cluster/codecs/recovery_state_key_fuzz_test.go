package codecs_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron/cnc"
	"github.com/warrenguy/heron/cluster/codecs"
)

func FuzzRecoveryStateKeyDecode(f *testing.F) {
	for _, name := range []string{
		"snapshot-two-services.key",
		"term-only.key",
		"max-services.key",
	} {
		key := readRecoveryStateKeyGolden(f, name, cnc.CounterMetadataMaxKeyLength)
		f.Add(key, uint8(0))
		f.Add(append(make([]byte, 7), key...), uint8(7))
	}

	f.Fuzz(func(t *testing.T, data []byte, offset uint8) {
		off := int(offset)
		fields, ok := codecs.TryDecodeRecoveryStateKeyAt(data, off)
		if !ok {
			return
		}

		// A decoded key re-encodes to the bytes the decode consumed.
		keyLen := 32 + 8*len(fields.SnapshotRecordingIDs)
		buf := make([]byte, keyLen)
		keyEnd, ok := codecs.EncodeRecoveryStateKey(buf, 0, fields)
		require.True(t, ok)
		require.Equal(t, keyLen, keyEnd)
		require.Equal(t, data[off:off+keyLen], buf)
	})
}

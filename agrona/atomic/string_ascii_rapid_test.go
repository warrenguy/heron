//go:build rapid

package atomic_test

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"

	"github.com/warrenguy/heron/agrona/atomic"
	"github.com/warrenguy/heron/internal/testutil"
)

func TestPutGetStringASCII_roundtrip_rapid(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		value := testutil.DrawASCIIString(rt, "value", 512)
		offset := rapid.IntRange(0, 8).Draw(rt, "offset")
		encodedLength := atomic.StringASCIIEncodedLength(len(value))
		buffer := atomic.Wrap(make([]byte, offset+encodedLength))
		prefix := testutil.SnapshotBufferPrefix(buffer, offset)

		gotEncodedLength, err := atomic.PutStringASCII(buffer, offset, value)
		require.NoError(rt, err)
		require.Equal(rt, encodedLength, gotEncodedLength)
		require.True(rt, bytes.Equal(prefix, buffer.Bytes()[:offset]))

		got, decodedLength, err := atomic.GetStringASCII(buffer, offset)
		require.NoError(rt, err)
		require.Equal(rt, value, got)
		require.Equal(rt, gotEncodedLength, decodedLength)
	})
}

func TestPutStringASCII_rejectsNonASCII_rapid(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		value := testutil.DrawNonASCIIString(rt, "value", 256)
		offset := rapid.IntRange(0, 8).Draw(rt, "offset")
		buffer := atomic.Wrap(make([]byte, offset+atomic.StringASCIIEncodedLength(len(value))))
		prefix := testutil.SnapshotBufferPrefix(buffer, offset)

		_, err := atomic.PutStringASCII(buffer, offset, value)
		require.ErrorIs(rt, err, atomic.ErrStringASCIINonASCII)
		require.EqualError(rt, err, "atomic: string ascii non-ascii")
		require.True(rt, bytes.Equal(prefix, buffer.Bytes()[:offset]))
	})
}

func TestStringASCIIEncodedLength_rapid(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		n := rapid.IntRange(0, 1024).Draw(rt, "n")
		require.Equal(rt, 4+n, atomic.StringASCIIEncodedLength(n))
	})
}

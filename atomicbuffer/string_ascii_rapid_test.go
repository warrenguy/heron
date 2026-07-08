//go:build rapid

package atomicbuffer_test

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"

	"github.com/warrenguy/heron/atomicbuffer"
	"github.com/warrenguy/heron/internal/testutil"
)

func TestPutGetStringASCII_roundtrip_rapid(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(rt *rapid.T) {
		value := testutil.DrawASCIIString(rt, "value", 512)
		offset := rapid.IntRange(0, 8).Draw(rt, "offset")
		encodedLength := atomicbuffer.StringASCIIEncodedLength(len(value))
		buffer := atomicbuffer.Wrap(make([]byte, offset+encodedLength))
		prefix := testutil.SnapshotBufferPrefix(buffer, offset)

		gotEncodedLength, err := atomicbuffer.PutStringASCII(buffer, offset, value)
		require.NoError(rt, err)
		require.Equal(rt, encodedLength, gotEncodedLength)
		require.True(rt, bytes.Equal(prefix, buffer.Bytes()[:offset]))

		got, decodedLength, err := atomicbuffer.GetStringASCII(buffer, offset)
		require.NoError(rt, err)
		require.Equal(rt, value, got)
		require.Equal(rt, gotEncodedLength, decodedLength)
	})
}

func TestPutStringASCII_rejectsNonASCII_rapid(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(rt *rapid.T) {
		value := testutil.DrawNonASCIIString(rt, "value", 256)
		offset := rapid.IntRange(0, 8).Draw(rt, "offset")
		encodedLength := atomicbuffer.StringASCIIEncodedLength(len(value))
		buffer := atomicbuffer.Wrap(make([]byte, offset+encodedLength))
		prefix := testutil.SnapshotBufferPrefix(buffer, offset)

		_, err := atomicbuffer.PutStringASCII(buffer, offset, value)
		require.ErrorIs(rt, err, atomicbuffer.ErrStringASCIINonASCII)
		require.EqualError(rt, err, "atomicbuffer: string ascii non-ascii")
		require.True(rt, bytes.Equal(prefix, buffer.Bytes()[:offset]))
	})
}

func TestStringASCIIEncodedLength_rapid(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(rt *rapid.T) {
		n := rapid.IntRange(0, 1024).Draw(rt, "n")
		require.Equal(rt, 4+n, atomicbuffer.StringASCIIEncodedLength(n))
	})
}

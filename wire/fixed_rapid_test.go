//go:build rapid

package wire_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"

	"github.com/warrenguy/heron/atomicbuffer"
	"github.com/warrenguy/heron/wire"
)

func TestPutGetInt64_roundtrip_rapid(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(rt *rapid.T) {
		capacity := rapid.IntRange(atomicbuffer.SizeInt64, 256).Draw(rt, "capacity")
		offset := rapid.IntRange(0, capacity-atomicbuffer.SizeInt64).Draw(rt, "offset")
		value := rapid.Int64().Draw(rt, "value")

		payload := make([]byte, capacity)
		wire.PutInt64(payload, offset, value)
		require.Equal(rt, value, wire.GetInt64(payload, offset))
	})
}

func TestPutGetBoolByte_roundtrip_rapid(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(rt *rapid.T) {
		capacity := rapid.IntRange(1, 256).Draw(rt, "capacity")
		offset := rapid.IntRange(0, capacity-1).Draw(rt, "offset")
		value := rapid.Bool().Draw(rt, "value")

		buf := atomicbuffer.Wrap(make([]byte, capacity))
		wire.PutBoolByte(buf, offset, value)
		require.Equal(rt, value, wire.GetBoolByte(buf, offset))
	})
}

func TestPutGetBoolByteBytes_roundtrip_rapid(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(rt *rapid.T) {
		capacity := rapid.IntRange(1, 256).Draw(rt, "capacity")
		offset := rapid.IntRange(0, capacity-1).Draw(rt, "offset")
		value := rapid.Bool().Draw(rt, "value")

		payload := make([]byte, capacity)
		wire.PutBoolByteBytes(payload, offset, value)
		require.Equal(rt, value, wire.GetBoolByteBytes(payload, offset))
	})
}

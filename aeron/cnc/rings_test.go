package cnc_test

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron/cnc"
	"github.com/warrenguy/heron/aeron/cnc/cnctest"
	"github.com/warrenguy/heron/ringbuffer"
)

const (
	ringBelowMinToDriverBufferLength int32 = ringbuffer.TrailerLength + 4
	ringNonPowerOfTwoToDriverLength  int32 = ringbuffer.TrailerLength + 777
)

func TestWireRings_readySynthetic(t *testing.T) {
	mapped := openRingReadyMappedCncFile(t)
	meta := mapped.Metadata()

	rings, err := cnc.WireRings(mapped)
	require.NoError(t, err)
	require.NotNil(t, rings.Mapping)
	require.Equal(t, meta, rings.Meta)
	cnctest.AssertWiredRingsReady(t, mapped, rings)
}

func TestWireRings_nilMappedFile(t *testing.T) {
	_, err := cnc.WireRings(nil)
	require.EqualError(t, err, "cnc: nil mapped file")
}

func TestWireRings_closedMappedFile(t *testing.T) {
	mapped := openRingReadyMappedCncFile(t)
	require.NoError(t, mapped.Close())

	_, err := cnc.WireRings(mapped)
	require.EqualError(t, err, "cnc: closed mapped file")
}

func TestWireRings_shortFileBounds(t *testing.T) {
	meta := cnc.RingReadyMetadata()
	cases := []struct {
		name      string
		shortFile int
		wantErr   string
	}{
		{
			name:      "to-driver",
			shortFile: cnc.MetaDataLength + 64,
			wantErr: fmt.Sprintf(
				"cnc: to-driver buffer out of bounds: start=%d length=%d file=%d",
				cnc.ToDriverOffset(),
				meta.ToDriverBufferLength,
				cnc.MetaDataLength+64,
			),
		},
		{
			name:      "to-clients",
			shortFile: cnc.ToClientsOffset(meta) + 32,
			wantErr: fmt.Sprintf(
				"cnc: to-clients buffer out of bounds: start=%d length=%d file=%d",
				cnc.ToClientsOffset(meta),
				meta.ToClientsBufferLength,
				cnc.ToClientsOffset(meta)+32,
			),
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			mapping := openSyntheticMapping(t, tc.shortFile, meta)

			_, err := cnc.WireRingsFromMapping(mapping, meta)
			require.EqualError(t, err, tc.wantErr)
		})
	}
}

func TestWireRings_nonPositiveBufferLength(t *testing.T) {
	baseMeta := cnc.RingReadyMetadata()
	mapping := openSyntheticMapping(t, baseMeta.ComputedLength(), baseMeta)

	cases := []struct {
		name    string
		setup   func(*cnc.Metadata)
		wantErr string
	}{
		{
			name: "to-driver zero",
			setup: func(meta *cnc.Metadata) {
				meta.ToDriverBufferLength = 0
			},
			wantErr: "cnc: to-driver buffer length must be positive: 0",
		},
		{
			name: "to-clients negative",
			setup: func(meta *cnc.Metadata) {
				meta.ToClientsBufferLength = -1
			},
			wantErr: "cnc: to-clients buffer length must be positive: -1",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			meta := baseMeta
			tc.setup(&meta)
			writeMetadataToBuffer(t, mapping.Data, meta)

			_, err := cnc.WireRingsFromMapping(mapping, meta)
			require.EqualError(t, err, tc.wantErr)
		})
	}
}

func TestWireRings_sharedMappingCloseInvalidatesRegion(t *testing.T) {
	mapped := openRingReadyMappedCncFile(t)

	rings, err := cnc.WireRings(mapped)
	require.NoError(t, err)
	require.NotNil(t, rings.Mapping)

	require.NoError(t, mapped.Close())
	require.Nil(t, rings.Mapping.Data)
	require.Nil(t, mapped.ToDriverBuffer())
}

func TestWireRingsFromMapping_nilMapping(t *testing.T) {
	_, err := cnc.WireRingsFromMapping(nil, cnc.RingReadyMetadata())
	require.EqualError(t, err, "cnc: closed mapped file")
}

func TestWireRings_invalidCapacity(t *testing.T) {
	cases := []struct {
		name    string
		setup   func(*cnc.Metadata)
		wantErr string
	}{
		{
			name: "to-driver below minimum",
			setup: func(meta *cnc.Metadata) {
				meta.ToDriverBufferLength = ringBelowMinToDriverBufferLength
			},
			wantErr: "cnc: to-driver ring: ringbuffer: insufficient capacity",
		},
		{
			name: "to-driver not power of two",
			setup: func(meta *cnc.Metadata) {
				meta.ToDriverBufferLength = ringNonPowerOfTwoToDriverLength
			},
			wantErr: "cnc: to-driver ring: ringbuffer: data capacity must be power of two",
		},
		{
			name: "to-clients not power of two",
			setup: func(meta *cnc.Metadata) {
				meta.ToClientsBufferLength = 777
			},
			wantErr: "cnc: to-clients broadcast: broadcast: data capacity must be power of two",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			meta := cnc.RingReadyMetadata()
			tc.setup(&meta)
			mapped := openMappedCncFile(t, meta)

			_, err := cnc.WireRings(mapped)
			require.EqualError(t, err, tc.wantErr)
		})
	}
}

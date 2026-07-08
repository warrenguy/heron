package cnc

import (
	"fmt"

	"github.com/warrenguy/heron/atomicbuffer"
	"github.com/warrenguy/heron/broadcast"
	"github.com/warrenguy/heron/mmap"
	"github.com/warrenguy/heron/ringbuffer"
)

var ringWireRegionIDs = [2]int{cncRegionToDriver, cncRegionToClients}

// Rings pins the CNC mmap region and wires to-driver / to-clients buffers.
//
// Do not call MappedCncFile.Close or Rings.Mapping.Close while using ToDriver
// or ToClients.
//
// Rings.Mapping retains the region if MappedCncFile is dropped without Close.
type Rings struct {
	Mapping   *mmap.Mapping
	Meta      Metadata
	ToDriver  *ringbuffer.ManyToOneRingBuffer
	ToClients *broadcast.BroadcastReceiver
}

// WireRings constructs ring and broadcast receivers over a mapped cnc.dat.
//
// Rings shares Mapping with mapped; see Rings for lifetime rules.
func WireRings(mapped *MappedCncFile) (*Rings, error) {
	if mapped == nil {
		return nil, fmt.Errorf("cnc: nil mapped file")
	}
	data, meta := mapped.mappedData()
	mapping := mapped.mapping
	if data == nil || mapping == nil {
		return nil, fmt.Errorf("cnc: closed mapped file")
	}
	return wireRings(mapping, meta)
}

func wireRings(mapping *mmap.Mapping, meta Metadata) (*Rings, error) {
	data := mapping.Data
	descs := bufferLengthDescriptors(meta)

	var bounds [2]struct {
		start int
		end   int
	}
	for i, regionID := range ringWireRegionIDs {
		start, end := cncRegionBounds(meta, regionID)
		bounds[i].start = start
		bounds[i].end = end
		if err := validateRegionBounds(data, start, end-start, descs[i].name); err != nil {
			return nil, err
		}
	}

	toDriver, err := ringbuffer.NewManyToOneRingBuffer(
		atomicbuffer.Wrap(data[bounds[0].start:bounds[0].end]),
	)
	if err != nil {
		return nil, fmt.Errorf("cnc: to-driver ring: %w", err)
	}

	toClients, err := broadcast.NewBroadcastReceiver(
		atomicbuffer.Wrap(data[bounds[1].start:bounds[1].end]),
	)
	if err != nil {
		return nil, fmt.Errorf("cnc: to-clients broadcast: %w", err)
	}

	return &Rings{
		Mapping:   mapping,
		Meta:      meta,
		ToDriver:  toDriver,
		ToClients: toClients,
	}, nil
}

func validateRegionBounds(data []byte, start, length int, name string) error {
	if length <= 0 {
		return fmt.Errorf("cnc: %s buffer length must be positive: %d", name, length)
	}
	end := start + length
	if end < start || end > len(data) {
		return fmt.Errorf(
			"cnc: %s buffer out of bounds: start=%d length=%d file=%d",
			name,
			start,
			length,
			len(data),
		)
	}
	return nil
}

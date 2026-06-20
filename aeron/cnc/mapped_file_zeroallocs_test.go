package cnc_test

import (
	"testing"

	"github.com/warrenguy/heron/aeron/cnc"
	"github.com/warrenguy/heron/internal/testutil"
)

var sink any

func TestMappedCncFile_Accessors_ZeroAllocs(t *testing.T) {
	mapped := openReadyMappedCncFile(t)

	cases := []struct {
		name string
		fn   func(*cnc.MappedCncFile)
	}{
		{
			name: "ToDriverBuffer",
			fn: func(mapped *cnc.MappedCncFile) {
				buf := mapped.ToDriverBuffer()
				sink = buf[0]
			},
		},
		{
			name: "ToClientsBuffer",
			fn: func(mapped *cnc.MappedCncFile) {
				buf := mapped.ToClientsBuffer()
				sink = buf[0]
			},
		},
		{
			name: "ToDriverAtomic",
			fn: func(mapped *cnc.MappedCncFile) {
				sink = mapped.ToDriverAtomic().GetInt32Volatile(0)
			},
		},
		{
			name: "ToClientsAtomic",
			fn: func(mapped *cnc.MappedCncFile) {
				sink = mapped.ToClientsAtomic().GetInt32Volatile(0)
			},
		},
		{
			name: "CountersMetaDataBuffer",
			fn: func(mapped *cnc.MappedCncFile) {
				buf := mapped.CountersMetaDataBuffer()
				sink = buf[0]
			},
		},
		{
			name: "CountersValuesBuffer",
			fn: func(mapped *cnc.MappedCncFile) {
				buf := mapped.CountersValuesBuffer()
				sink = buf[0]
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			testutil.AssertZeroAllocs(t, tc.name, 1000, func() {
				tc.fn(mapped)
			})
		})
	}
}

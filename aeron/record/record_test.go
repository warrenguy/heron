package record_test

import (
	"errors"
	"testing"

	"github.com/warrenguy/heron/aeron/record"
)

func TestRecord(t *testing.T) {
	const recordOffset = 16

	t.Run("constants", func(t *testing.T) {
		cases := []struct {
			name string
			got  int
			want int
		}{
			{"HeaderLength", record.HeaderLength, 8},
			{"Alignment", record.Alignment, record.HeaderLength},
			{"TypeFieldOffset", record.TypeFieldOffset, 4},
			{"PaddingMsgTypeID", record.PaddingMsgTypeID, -1},
			{"InsufficientCapacity", record.InsufficientCapacity, -2},
		}
		for _, tc := range cases {
			if tc.got != tc.want {
				t.Fatalf("%s=%d want %d", tc.name, tc.got, tc.want)
			}
		}
	})

	t.Run("offsets", func(t *testing.T) {
		cases := []struct {
			name string
			got  int
			want int
		}{
			{"LengthOffset", record.LengthOffset(recordOffset), recordOffset},
			{"TypeOffset", record.TypeOffset(recordOffset), recordOffset + record.TypeFieldOffset},
			{"EncodedMsgOffset", record.EncodedMsgOffset(recordOffset), recordOffset + record.HeaderLength},
		}
		for _, tc := range cases {
			if tc.got != tc.want {
				t.Fatalf("%s=%d want %d", tc.name, tc.got, tc.want)
			}
		}
	})

	t.Run("AlignRecordLength", func(t *testing.T) {
		if got := record.AlignRecordLength(9); got != 16 {
			t.Fatalf("AlignRecordLength(9)=%d want 16", got)
		}
	})

	t.Run("CheckTypeID", func(t *testing.T) {
		if err := record.CheckTypeID(1); err != nil {
			t.Fatal(err)
		}
		for _, id := range []int{0, -1} {
			err := record.CheckTypeID(id)
			if err == nil {
				t.Fatalf("expected error for type id %d", id)
			}
			if !errors.Is(err, record.ErrInvalidMessageTypeID) {
				t.Fatalf("CheckTypeID(%d)=%v want ErrInvalidMessageTypeID", id, err)
			}
		}
	})
}

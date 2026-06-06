package broadcast_test

import (
	"testing"

	"github.com/warrenguy/heron/aeron/broadcast"
	"github.com/warrenguy/heron/aeron/internal/testutil"
)

func TestBroadcast_ZeroAllocs(t *testing.T) {
	cases := []struct {
		name  string
		runs  int
		warm  func(testing.TB, *broadcast.BroadcastTransmitter, *broadcast.BroadcastReceiver)
		setup func(testing.TB, *broadcast.BroadcastTransmitter, *broadcast.BroadcastReceiver)
		fn    func(*broadcast.BroadcastTransmitter, *broadcast.BroadcastReceiver)
	}{
		{
			name: "ReceiveNext",
			warm: warmTransmitOnly,
			fn: func(tx *broadcast.BroadcastTransmitter, rx *broadcast.BroadcastReceiver) {
				if err := tx.Transmit(testMsgTypeID, testPayload8); err != nil {
					panic(err)
				}
				if !rx.ReceiveNext() {
					panic("ReceiveNext=false")
				}
			},
		},
		{
			name: "Validate",
			warm: warmTransmitReceive,
			fn: func(_ *broadcast.BroadcastTransmitter, rx *broadcast.BroadcastReceiver) {
				_ = rx.Validate()
			},
		},
		{
			name: "TypeID/Offset/Length",
			warm: warmTransmitReceive,
			fn: func(_ *broadcast.BroadcastTransmitter, rx *broadcast.BroadcastReceiver) {
				_ = rx.TypeID()
				_ = rx.Offset()
				_ = rx.Length()
			},
		},
		{
			name: "Transmit",
			warm: warmTransmitOnly,
			fn: func(tx *broadcast.BroadcastTransmitter, _ *broadcast.BroadcastReceiver) {
				if err := tx.Transmit(testMsgTypeID, testPayload8); err != nil {
					panic(err)
				}
			},
		},
		{
			name: "LappedCount",
			warm: warmTransmitReceive,
			fn: func(_ *broadcast.BroadcastTransmitter, rx *broadcast.BroadcastReceiver) {
				_ = rx.LappedCount()
			},
		},
		{
			name: "ReceiveNextOnly",
			runs: 64,
			setup: func(tb testing.TB, tx *broadcast.BroadcastTransmitter, _ *broadcast.BroadcastReceiver) {
				const preFill = 64
				for range preFill {
					warmTransmit(tb, tx)
				}
			},
			fn: func(_ *broadcast.BroadcastTransmitter, rx *broadcast.BroadcastReceiver) {
				_ = rx.ReceiveNext()
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tx, rx, _ := newTestBroadcastPair(t)
			if tc.warm != nil {
				tc.warm(t, tx, rx)
			}
			if tc.setup != nil {
				tc.setup(t, tx, rx)
			}
			runs := tc.runs
			if runs == 0 {
				runs = 1000
			}
			testutil.AssertZeroAllocs(t, tc.name, runs, func() {
				tc.fn(tx, rx)
			})
		})
	}
}

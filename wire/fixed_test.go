package wire_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/atomicbuffer"
	"github.com/warrenguy/heron/internal/testutil"
	"github.com/warrenguy/heron/wire"
)

const atomicbufferBoundsPanic = "atomicbuffer: offset out of bounds"

// Local auction-layout constants (mirrors auctionwire; no examples/ import).
const (
	testCorrelationOffset = 0
	testCustomerOffset    = 8
	testPriceOffset       = 16
	testBidLength         = 24
	testBoolOffset        = testBidLength
	testEgressLength      = testBidLength + 1
)

type testEgressBid struct {
	CorrelationID, CustomerID, Price int64
	BidSucceeded                     bool
}

func decodeUsingWireHelpers(payload []byte, offset, length int) (testEgressBid, bool) {
	if !wire.CheckLength(length, testEgressLength) {
		return testEgressBid{}, false
	}
	return testEgressBid{
		CorrelationID: wire.GetInt64(payload, offset+testCorrelationOffset),
		CustomerID:    wire.GetInt64(payload, offset+testCustomerOffset),
		Price:         wire.GetInt64(payload, offset+testPriceOffset),
		BidSucceeded:  wire.GetBoolByteBytes(payload, offset+testBoolOffset),
	}, true
}

func TestCheckLength(t *testing.T) {
	cases := []struct {
		name    string
		length  int
		minimum int
		want    bool
	}{
		{name: "exact", length: 25, minimum: 25, want: true},
		{name: "over", length: 30, minimum: 25, want: true},
		{name: "under", length: 24, minimum: 25, want: false},
		{name: "zero", length: 0, minimum: 1, want: false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, wire.CheckLength(tc.length, tc.minimum))
		})
	}
}

func TestCheckLength_passes_but_field_read_panics(t *testing.T) {
	const (
		reportedLength  = testEgressLength
		boolFieldOffset = testBoolOffset
	)

	t.Run("offset_zero", func(t *testing.T) {
		buf := atomicbuffer.Wrap(make([]byte, reportedLength-1))

		require.True(t, wire.CheckLength(reportedLength, reportedLength))
		require.PanicsWithValue(t, atomicbufferBoundsPanic, func() {
			wire.GetBoolByte(buf, boolFieldOffset)
		})
	})

	t.Run("non_zero_offset", func(t *testing.T) {
		const offset = 4
		buf := atomicbuffer.Wrap(make([]byte, offset+reportedLength-1))

		require.True(t, wire.CheckLength(reportedLength, reportedLength))
		require.PanicsWithValue(t, atomicbufferBoundsPanic, func() {
			wire.GetBoolByte(buf, offset+boolFieldOffset)
		})
	})
}

func TestInt64_roundtrip(t *testing.T) {
	const (
		correlationID int64 = 100
		customerID    int64 = 200
		price         int64 = 300
	)
	t.Run("put_get_roundtrip_offset_zero", func(t *testing.T) {
		payload := make([]byte, testBidLength)
		wire.PutInt64(payload, testCorrelationOffset, correlationID)
		wire.PutInt64(payload, testCustomerOffset, customerID)
		wire.PutInt64(payload, testPriceOffset, price)
		require.Equal(t, correlationID, wire.GetInt64(payload, testCorrelationOffset))
		require.Equal(t, customerID, wire.GetInt64(payload, testCustomerOffset))
		require.Equal(t, price, wire.GetInt64(payload, testPriceOffset))
	})

	t.Run("put_get_non_zero_offset", func(t *testing.T) {
		const offset = 4
		payload := make([]byte, offset+testBidLength)
		wire.PutInt64(payload, offset+testCorrelationOffset, correlationID)
		wire.PutInt64(payload, offset+testCustomerOffset, customerID)
		wire.PutInt64(payload, offset+testPriceOffset, price)
		require.Equal(t, correlationID, wire.GetInt64(payload, offset+testCorrelationOffset))
		require.Equal(t, customerID, wire.GetInt64(payload, offset+testCustomerOffset))
		require.Equal(t, price, wire.GetInt64(payload, offset+testPriceOffset))
	})
}

func TestInt64_oob_panics(t *testing.T) {
	payload := make([]byte, atomicbuffer.SizeInt64)
	cases := []struct {
		name string
		fn   func()
	}{
		{
			name: "get_at_len_minus_seven",
			fn:   func() { wire.GetInt64(payload, len(payload)-7) },
		},
		{
			name: "put_at_len_minus_seven",
			fn:   func() { wire.PutInt64(payload, len(payload)-7, 1) },
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.PanicsWithValue(t, atomicbufferBoundsPanic, tc.fn)
		})
	}
}

func TestBoolByte_buffer_roundtrip(t *testing.T) {
	cases := []struct {
		name   string
		offset int
		bufLen int
	}{
		{name: "offset_zero", offset: 0, bufLen: 8},
		{name: "offset_non_zero", offset: 4, bufLen: 16},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			buf := atomicbuffer.Wrap(make([]byte, tc.bufLen))

			wire.PutBoolByte(buf, tc.offset, false)
			require.False(t, wire.GetBoolByte(buf, tc.offset))
			require.Equal(t, byte(0), buf.GetByte(tc.offset))

			wire.PutBoolByte(buf, tc.offset, true)
			require.True(t, wire.GetBoolByte(buf, tc.offset))
			require.Equal(t, byte(1), buf.GetByte(tc.offset))
		})
	}
}

func TestBoolByte_bytes_roundtrip(t *testing.T) {
	payload := make([]byte, 8)

	wire.PutBoolByteBytes(payload, 0, false)
	require.False(t, wire.GetBoolByteBytes(payload, 0))
	require.Equal(t, byte(0), payload[0])

	wire.PutBoolByteBytes(payload, 0, true)
	require.True(t, wire.GetBoolByteBytes(payload, 0))
	require.Equal(t, byte(1), payload[0])
}

func TestBoolByte_non_zero_ff_is_true(t *testing.T) {
	payload := make([]byte, 1)
	payload[0] = 0xff
	require.True(t, wire.GetBoolByteBytes(payload, 0))
	require.True(t, wire.GetBoolByte(atomicbuffer.Wrap(payload), 0))
}

type boolOOBAccessors struct {
	get            func(offset int)
	put            func(offset int)
	capacityOffset int
	getNil         func()
	putNil         func()
}

func runBoolOOBPanics(t *testing.T, acc boolOOBAccessors) {
	t.Helper()

	cases := []struct {
		name string
		fn   func()
	}{
		{"get_at_capacity", func() { acc.get(acc.capacityOffset) }},
		{"get_negative_offset", func() { acc.get(-1) }},
		{"put_at_capacity", func() { acc.put(acc.capacityOffset) }},
		{"put_negative_offset", func() { acc.put(-1) }},
		{"get_nil", acc.getNil},
		{"put_nil", acc.putNil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.PanicsWithValue(t, atomicbufferBoundsPanic, tc.fn)
		})
	}
}

func TestBoolByte_oob_panics(t *testing.T) {
	t.Run("buffer", func(t *testing.T) {
		byteBuffer := atomicbuffer.Wrap(make([]byte, atomicbuffer.SizeByte))
		emptyBuffer := atomicbuffer.Wrap(nil)
		runBoolOOBPanics(t, boolOOBAccessors{
			get:            func(offset int) { wire.GetBoolByte(byteBuffer, offset) },
			put:            func(offset int) { wire.PutBoolByte(byteBuffer, offset, true) },
			capacityOffset: byteBuffer.Capacity(),
			getNil:         func() { wire.GetBoolByte(emptyBuffer, 0) },
			putNil:         func() { wire.PutBoolByte(emptyBuffer, 0, true) },
		})
	})

	t.Run("bytes", func(t *testing.T) {
		payload := make([]byte, atomicbuffer.SizeByte)
		runBoolOOBPanics(t, boolOOBAccessors{
			get:            func(offset int) { wire.GetBoolByteBytes(payload, offset) },
			put:            func(offset int) { wire.PutBoolByteBytes(payload, offset, true) },
			capacityOffset: len(payload),
			getNil:         func() { wire.GetBoolByteBytes(nil, 0) },
			putNil:         func() { wire.PutBoolByteBytes(nil, 0, true) },
		})
	})
}

// Vectors mirror examples/cluster-auction-client/auctionwire/wire_test.go TestDecodeEgress.
func TestAuctionLayout_egressRoundtrip(t *testing.T) {
	const (
		correlationID int64 = 11
		customerID    int64 = 22
		price         int64 = 33
	)
	wantBid := testEgressBid{
		CorrelationID: correlationID,
		CustomerID:    customerID,
		Price:         price,
		BidSucceeded:  true,
	}
	cases := []struct {
		name         string
		offset       int
		length       int
		bidSucceeded byte
		want         testEgressBid
		wantOK       bool
	}{
		{
			name: "valid_at_offset_zero", offset: 0,
			length: testEgressLength, bidSucceeded: 1,
			want: wantBid, wantOK: true,
		},
		{
			name: "valid_at_non_zero_offset", offset: 4,
			length: testEgressLength, bidSucceeded: 1,
			want: wantBid, wantOK: true,
		},
		{name: "short", offset: 0, length: testEgressLength - 1, wantOK: false},
		{
			name: "bid_succeeded_zero", offset: 0,
			length: testEgressLength, bidSucceeded: 0,
			want: testEgressBid{
				CorrelationID: correlationID,
				CustomerID:    customerID,
				Price:         price,
			},
			wantOK: true,
		},
		{
			name: "bid_succeeded_non_zero", offset: 0,
			length: testEgressLength, bidSucceeded: 0xff,
			want: wantBid, wantOK: true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			payload := make([]byte, tc.offset+tc.length)
			if tc.wantOK {
				wire.PutInt64(payload, tc.offset+testCorrelationOffset, correlationID)
				wire.PutInt64(payload, tc.offset+testCustomerOffset, customerID)
				wire.PutInt64(payload, tc.offset+testPriceOffset, price)
				payload[tc.offset+testBoolOffset] = tc.bidSucceeded
			}

			got, ok := decodeUsingWireHelpers(payload, tc.offset, tc.length)
			require.Equal(t, tc.wantOK, ok)
			if tc.wantOK {
				require.Equal(t, tc.want, got)
			}
		})
	}
}

func TestWirePrimitives_zeroAllocs(t *testing.T) {
	payload := make([]byte, 32)
	buf := atomicbuffer.Wrap(payload)
	var i64 int64

	cases := []struct {
		name string
		fn   func()
	}{
		{"CheckLength", func() { _ = wire.CheckLength(25, testEgressLength) }},
		{"GetInt64", func() { _ = wire.GetInt64(payload, 0) }},
		{"PutInt64", func() {
			wire.PutInt64(payload, 0, i64)
			i64++
		}},
		{"GetBoolByte", func() { _ = wire.GetBoolByte(buf, 0) }},
		{"PutBoolByte", func() { wire.PutBoolByte(buf, 0, i64&1 == 0) }},
		{"GetBoolByteBytes", func() { _ = wire.GetBoolByteBytes(payload, 0) }},
		{"PutBoolByteBytes", func() { wire.PutBoolByteBytes(payload, 0, i64&1 == 0) }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			testutil.AssertZeroAllocs(t, tc.name, 1000, tc.fn)
		})
	}
}

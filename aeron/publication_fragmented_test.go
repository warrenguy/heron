package aeron_test

import (
	"bytes"
	"fmt"
	"hash/crc32"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/aeron/logbuffer"
	"github.com/warrenguy/heron/atomicbuffer"
)

// fragmentTB is the testing surface used by fragmented-message helpers. Embeds
// require.TestingT plus Helper. Not full testing.TB.
type fragmentTB interface {
	Helper()
	require.TestingT
}

// fragmentedMessageFrameLocation returns the term partition index and start
// offset of a fragmented or single-frame message that ended at position.
// Callers select the term buffer for their publication type.
func fragmentedMessageFrameLocation(
	position int64,
	length, maxPayload int,
) (termIndex, frameStart int) {
	shift := logbuffer.PositionBitsToShift(aeron.TestPublicationTermLength)
	totalAligned := logbuffer.ComputeFragmentedFrameLength(length, maxPayload)
	return logbuffer.IndexByPosition(position, shift),
		aeron.FragmentedMessageStartOffsetForTest(position, shift, totalAligned)
}

func readFragmentedFramesAtPosition(
	tb fragmentTB,
	pub *aeron.Publication,
	position int64,
	length, maxPayload int,
) []aeron.FragmentedFrameForTest {
	tb.Helper()

	termIndex, frameStart := fragmentedMessageFrameLocation(position, length, maxPayload)
	termBuf := aeron.PublicationTermBufferForTest(pub, termIndex)
	return aeron.ReadFragmentedMessageForTest(tb, termBuf, frameStart)
}

func requireReassembledPayload(
	tb fragmentTB,
	frames []aeron.FragmentedFrameForTest,
	want []byte,
) {
	tb.Helper()
	require.Equal(tb, want, reassembledPayload(frames))
}

func reassembledPayload(frames []aeron.FragmentedFrameForTest) []byte {
	var got []byte
	for _, frame := range frames {
		got = append(got, frame.Payload...)
	}
	return got
}

func collectCompleteFragmentedPayloads(
	tb testing.TB,
	pub *aeron.Publication,
	maxPayload int,
) [][]byte {
	tb.Helper()

	var payloads [][]byte
	for partition := range logbuffer.PartitionCount {
		rawTail, err := logbuffer.GetRawTail(pub.MetaDataBufferForTest(), partition)
		require.NoError(tb, err)

		termEnd := int(logbuffer.TermOffset(rawTail, aeron.TestPublicationTermLength))
		termBuf := aeron.PublicationTermBufferForTest(pub, partition)
		offset := 0
		for offset < termEnd {
			frameLength := int(logbuffer.FrameLengthVolatile(termBuf, offset))
			if frameLength <= 0 {
				offset += logbuffer.FrameAlignment
				continue
			}

			absLength := frameLength
			if absLength < 0 {
				absLength = -absLength
			}
			aligned := logbuffer.AlignFrameLength(absLength)

			if logbuffer.IsPaddingFrame(termBuf, offset) {
				offset += aligned
				continue
			}

			flags := logbuffer.FrameFlags(termBuf, offset)
			if flags&logbuffer.Unfragmented == logbuffer.Unfragmented {
				offset += aligned
				continue
			}

			if flags&logbuffer.BeginFlag != 0 && flags&logbuffer.EndFlag == 0 {
				frames, ok := aeron.ReadFragmentedMessageFramesForTest(termBuf, offset)
				if ok {
					payload := reassembledPayload(frames)
					requireFragmentFlags(tb, frames)
					payloads = append(payloads, payload)
					offset += logbuffer.ComputeFragmentedFrameLength(len(payload), maxPayload)
					continue
				}
			}

			offset += aligned
		}
	}

	return payloads
}

func requireCommittedFragmentedPayloads(
	tb testing.TB,
	pub *aeron.Publication,
	want []byte,
) {
	tb.Helper()

	maxPayload := pub.MaxPayloadLengthForTest()
	got := collectCompleteFragmentedPayloads(tb, pub, maxPayload)
	require.NotEmpty(
		tb,
		got,
		"expected at least one committed fragmented message in the log",
	)
	for _, payload := range got {
		require.Equal(tb, want, payload)
	}
}

func requireFragmentFlags(tb fragmentTB, frames []aeron.FragmentedFrameForTest) {
	tb.Helper()

	last := len(frames) - 1
	for i, frame := range frames {
		switch i {
		case 0:
			require.Equal(tb, uint8(logbuffer.BeginFlag), frame.Flags&logbuffer.BeginFlag)
			if last > 0 {
				require.Equal(tb, uint8(0), frame.Flags&logbuffer.EndFlag)
			}
		case last:
			require.Equal(tb, uint8(logbuffer.EndFlag), frame.Flags&logbuffer.EndFlag)
			if last > 0 {
				require.Equal(tb, uint8(0), frame.Flags&logbuffer.BeginFlag)
			}
		default:
			require.Equal(tb, uint8(0), frame.Flags&(logbuffer.BeginFlag|logbuffer.EndFlag))
		}
	}
}

// requireFragmentedMessageShape asserts fragment count, per-chunk payload
// bounds, the multi-frame flag train, and that no frame is Unfragmented when
// length exceeds maxPayload.
func requireFragmentedMessageShape(
	tb fragmentTB,
	frames []aeron.FragmentedFrameForTest,
	length, maxPayload int,
) {
	tb.Helper()

	wantCount := (length + maxPayload - 1) / maxPayload
	require.Len(tb, frames, wantCount)
	requireFragmentFlags(tb, frames)
	for _, frame := range frames {
		require.LessOrEqual(tb, len(frame.Payload), maxPayload)
		if length > maxPayload {
			require.NotEqual(
				tb,
				uint8(logbuffer.Unfragmented),
				frame.Flags&logbuffer.Unfragmented,
			)
		}
	}
}

func TestPublication_FragmentedOffer_twoFrames(t *testing.T) {
	pub := aeron.NewTestPublication(t, aeron.DefaultTestPublicationLogConfig(), int64(1)<<40)
	maxPayload := pub.MaxPayloadLengthForTest()
	length := maxPayload + 1

	payload := patternedPayload(length)

	position := aeron.OfferRetryForTest(pub, payload)
	require.Positive(t, position)

	frames := readFragmentedFramesAtPosition(t, pub, position, length, maxPayload)
	requireFragmentedMessageShape(t, frames, length, maxPayload)
	require.Len(t, frames[0].Payload, maxPayload)
	require.Len(t, frames[1].Payload, 1)
	requireReassembledPayload(t, frames, payload)
}

func TestPublication_FragmentedOffer_threeFrames(t *testing.T) {
	pub := aeron.NewTestPublication(t, aeron.DefaultTestPublicationLogConfig(), int64(1)<<40)
	maxPayload := pub.MaxPayloadLengthForTest()
	length := 3 * maxPayload

	payload := make([]byte, length)
	for i := range payload {
		payload[i] = byte(0xA0 + (i % 16))
	}

	position := aeron.OfferRetryForTest(pub, payload)
	require.Positive(t, position)

	frames := readFragmentedFramesAtPosition(t, pub, position, length, maxPayload)
	requireFragmentedMessageShape(t, frames, length, maxPayload)

	for i, frame := range frames {
		require.Len(t, frame.Payload, maxPayload)
		start := i * maxPayload
		require.Equal(t, payload[start:start+maxPayload], frame.Payload)
	}
}

func TestPublication_FragmentedOffer_termRolloverRetry(t *testing.T) {
	maxPayload := int(aeron.TestPublicationMTULength) - logbuffer.DataHeaderLength
	margin := logbuffer.AlignFrameLength(maxPayload + logbuffer.DataHeaderLength)
	pub := newTestPublicationNearTermEnd(t, margin)

	length := maxPayload + 1
	payload := patternedPayload(length)

	first := pub.Offer(payload)
	require.Equal(t, aeron.AdminAction, first)
	require.Equal(t, int32(1), logbuffer.ActiveTermCount(pub.MetaDataBufferForTest()))

	termBuf := aeron.PublicationTermBufferForTest(pub, 0)
	paddingOffset := aeron.TestPublicationTermLength - margin
	require.True(t, logbuffer.IsPaddingFrame(termBuf, paddingOffset))
	require.Equal(t, int32(margin), logbuffer.FrameLengthVolatile(termBuf, paddingOffset))
	require.Equal(
		t,
		int32(0),
		logbuffer.FrameLengthVolatile(termBuf, paddingOffset+logbuffer.OffsetFramePayload),
	)

	position := aeron.OfferRetryForTest(pub, payload)
	require.Positive(t, position)

	shift := logbuffer.PositionBitsToShift(aeron.TestPublicationTermLength)
	require.NotEqual(
		t,
		0,
		logbuffer.IndexByPosition(position, shift),
	)

	frames := readFragmentedFramesAtPosition(t, pub, position, length, maxPayload)
	requireFragmentedMessageShape(t, frames, length, maxPayload)
	requireReassembledPayload(t, frames, payload)
}

func TestPublication_FragmentedOffer_withOffset(t *testing.T) {
	pub := aeron.NewTestPublication(t, aeron.DefaultTestPublicationLogConfig(), int64(1)<<40)
	maxPayload := pub.MaxPayloadLengthForTest()
	length := maxPayload + 1

	buffer := append([]byte("xx"), make([]byte, length)...)
	for i := 2; i < len(buffer); i++ {
		buffer[i] = byte(i)
	}

	position := aeron.OfferWithOffsetRetryForTest(pub, buffer, 2, length)
	require.Positive(t, position)

	frames := readFragmentedFramesAtPosition(t, pub, position, length, maxPayload)
	requireFragmentedMessageShape(t, frames, length, maxPayload)
	require.Len(t, frames[0].Payload, maxPayload)
	require.Len(t, frames[1].Payload, 1)
	requireReassembledPayload(t, frames, buffer[2:])
}

// offerVariantPublication is the offer surface that both publication types
// share.
type offerVariantPublication interface {
	OfferWithOffset(buffer []byte, offset, length int) int64
	OfferWithReservedValue(
		buffer []byte,
		offset int,
		length int,
		supplier aeron.ReservedValueSupplier,
	) int64
	OfferTwoParts(first, second []byte, supplier aeron.ReservedValueSupplier) int64
	OfferVector(vectors [][]byte, supplier aeron.ReservedValueSupplier) int64
	Position() int64
	MaxPayloadLength() int
	MaxMessageLength() int
	TermBufferForTest(index int) atomicbuffer.AtomicBuffer
	Close() error
}

// offerVariantKind builds one publication type for the offer variant tests.
type offerVariantKind struct {
	name       string
	baseConfig func() aeron.TestPublicationLogConfig
	open       func(cfg aeron.TestPublicationLogConfig, limit int64) offerVariantPublication
}

// openUnlimited opens a publication on the base config with a limit that
// the tests never reach.
func (kind offerVariantKind) openUnlimited() offerVariantPublication {
	return kind.open(kind.baseConfig(), int64(1)<<62)
}

var offerVariantKinds = []offerVariantKind{
	{
		name:       "concurrent",
		baseConfig: aeron.DefaultTestPublicationLogConfig,
		open: func(cfg aeron.TestPublicationLogConfig, limit int64) offerVariantPublication {
			return aeron.NewTestPublicationWithoutTBForRapid(cfg, limit)
		},
	},
	{
		name:       "exclusive",
		baseConfig: aeron.DefaultTestExclusivePublicationLogConfig,
		open: func(cfg aeron.TestPublicationLogConfig, limit int64) offerVariantPublication {
			return aeron.NewTestExclusivePublicationWithoutTBForRapid(cfg, limit)
		},
	},
}

// offerVariantForm offers payload through one offer variant, split at the
// sorted cuts.
type offerVariantForm struct {
	name  string
	offer func(
		pub offerVariantPublication,
		payload []byte,
		cuts []int,
		supplier aeron.ReservedValueSupplier,
	) int64
}

var offerVariantForms = []offerVariantForm{
	{
		// len(cuts) guard bytes sit on each side of payload.
		name: "OfferWithReservedValue",
		offer: func(
			pub offerVariantPublication,
			payload []byte,
			cuts []int,
			supplier aeron.ReservedValueSupplier,
		) int64 {
			guard := bytes.Repeat([]byte{0xEE}, len(cuts))
			buffer := slices.Concat(guard, payload, guard)
			return pub.OfferWithReservedValue(buffer, len(guard), len(payload), supplier)
		},
	},
	{
		// The first cut splits the parts. With no cut the first part is
		// empty.
		name: "OfferTwoParts",
		offer: func(
			pub offerVariantPublication,
			payload []byte,
			cuts []int,
			supplier aeron.ReservedValueSupplier,
		) int64 {
			cut := 0
			if len(cuts) > 0 {
				cut = cuts[0]
			}
			return pub.OfferTwoParts(payload[:cut], payload[cut:], supplier)
		},
	},
	{
		// Every cut ends one part. Repeated cuts give empty parts.
		name: "OfferVector",
		offer: func(
			pub offerVariantPublication,
			payload []byte,
			cuts []int,
			supplier aeron.ReservedValueSupplier,
		) int64 {
			vectors := make([][]byte, 0, len(cuts)+1)
			start := 0
			for _, cut := range cuts {
				vectors = append(vectors, payload[start:cut])
				start = cut
			}
			vectors = append(vectors, payload[start:])
			return pub.OfferVector(vectors, supplier)
		},
	},
}

// offerVariantCuts returns the cuts of the table tests: three parts, one of
// them empty.
func offerVariantCuts(length int) []int {
	third := length / 3
	return []int{third, third, 2 * third}
}

// offerVariantLengths returns the message lengths that every offer variant
// row covers.
func offerVariantLengths(maxPayload, maxMessage int) []int {
	return []int{0, 64, maxPayload, maxPayload + 1, 3 * maxPayload, maxMessage, maxMessage + 1}
}

// firstDifference returns the first offset at which want and got differ. A
// failure message names it in place of a full term dump.
func firstDifference(want, got []byte) int {
	first := 0
	for first < min(len(want), len(got)) && want[first] == got[first] {
		first++
	}
	return first
}

// requireEqualTerm asserts that partition index of subject holds want.
func requireEqualTerm(tb fragmentTB, subject offerVariantPublication, index int, want []byte) {
	tb.Helper()

	got := subject.TermBufferForTest(index).Bytes()
	if !bytes.Equal(want, got) {
		require.Failf(tb, "term mismatch",
			"partition %d: first difference at offset %d", index, firstDifference(want, got))
	}
}

// requireEqualTerms asserts that every partition of subject holds the same
// bytes as the matching partition of reference.
func requireEqualTerms(tb fragmentTB, reference, subject offerVariantPublication) {
	tb.Helper()

	for index := range logbuffer.PartitionCount {
		requireEqualTerm(tb, subject, index, reference.TermBufferForTest(index).Bytes())
	}
}

// staleReservedValue is the word that fillStaleTerm leaves in every reserved
// value field: 0xA5A5A5A5A5A5A5A5 as an int64.
const staleReservedValue int64 = -0x5A5A5A5A5A5A5A5B

// fillStaleTerm fills every byte of term with 0xA5.
func fillStaleTerm(term []byte) {
	for i := range term {
		term[i] = 0xA5
	}
}

// crcReservedValue returns a CRC-32 of the frame payload in the low word and
// len(frame) in the high word.
func crcReservedValue(frame []byte) int64 {
	return int64(len(frame))<<32 | int64(crc32.ChecksumIEEE(frame[logbuffer.DataHeaderLength:]))
}

// offerFrameCount returns the DATA frame count of a message of length bytes.
func offerFrameCount(length, maxPayload int) int {
	return max(1, (length+maxPayload-1)/maxPayload)
}

// expectedFragmentFlags returns the BEGIN and END flags of frame index of a
// message with frameCount frames.
func expectedFragmentFlags(index, frameCount int) uint8 {
	var flags uint8
	if index == 0 {
		flags = logbuffer.BeginFlag
	}
	if index == frameCount-1 {
		flags |= logbuffer.EndFlag
	}
	return flags
}

// recordedFrame is the term location and length of one frame the supplier
// saw.
type recordedFrame struct {
	termID      int32
	termOffset  int
	frameLength int
}

// reservedValueRecorder supplies crcReservedValue for a message of frameCount
// frames. It asserts the supplier contract on each call and records the frame.
type reservedValueRecorder struct {
	tb         fragmentTB
	frameCount int
	frames     []recordedFrame
}

func (recorder *reservedValueRecorder) supply(frame []byte) int64 {
	tb := recorder.tb
	tb.Helper()

	index := len(recorder.frames)
	require.Less(tb, index, recorder.frameCount, "supplier calls")
	require.Equal(tb, len(frame), cap(frame))

	header := atomicbuffer.Wrap(frame)
	require.Equal(tb, int32(-len(frame)), header.GetInt32Volatile(logbuffer.OffsetFrameLength))
	require.Equal(tb, expectedFragmentFlags(index, recorder.frameCount), frame[logbuffer.OffsetFlags])

	recorder.frames = append(recorder.frames, recordedFrame{
		termID:      header.GetInt32(logbuffer.OffsetTermID),
		termOffset:  int(header.GetInt32(logbuffer.OffsetTermOffset)),
		frameLength: len(frame),
	})
	return crcReservedValue(frame)
}

// forEachDataFrame calls visit with the offset and length of each committed
// DATA frame below termEnd.
func forEachDataFrame(
	termBuf atomicbuffer.AtomicBuffer,
	termEnd int,
	visit func(offset, frameLength int),
) {
	for offset := 0; offset < termEnd; {
		frameLength := int(logbuffer.FrameLengthVolatile(termBuf, offset))
		if frameLength <= 0 {
			offset += logbuffer.FrameAlignment
			continue
		}
		if logbuffer.FrameType(termBuf, offset) == logbuffer.HdrTypeData {
			visit(offset, frameLength)
		}
		offset += logbuffer.AlignFrameLength(frameLength)
	}
}

// putExpectedReservedValue writes crcReservedValue of the frame at
// termOffset into its reserved value field.
func putExpectedReservedValue(term atomicbuffer.AtomicBuffer, termOffset int) {
	frameEnd := termOffset + int(logbuffer.FrameLength(term, termOffset))
	frame := term.Bytes()[termOffset:frameEnd]
	logbuffer.PutFrameReservedValue(term, termOffset, crcReservedValue(frame))
}

// expectedTerms copies the reference partitions. With a supplier it writes
// the expected reserved value into each DATA frame of the copy. Returns the
// copy and the DATA frame count.
func expectedTerms(
	reference offerVariantPublication,
	withSupplier bool,
) (terms [logbuffer.PartitionCount][]byte, dataFrames int) {
	for index := range logbuffer.PartitionCount {
		term := atomicbuffer.Wrap(bytes.Clone(reference.TermBufferForTest(index).Bytes()))
		forEachDataFrame(term, term.Capacity(), func(offset, _ int) {
			if withSupplier {
				putExpectedReservedValue(term, offset)
			}
			dataFrames++
		})
		terms[index] = term.Bytes()
	}
	return terms, dataFrames
}

// requireVariantOffer offers payload twice: on reference through
// OfferWithOffset, on subject through form. With a supplier the subject
// offer records each frame. It asserts equal outcomes, one supplier call per
// DATA frame, and equal terms once the reference copy holds the expected
// reserved values. Returns the outcome.
func requireVariantOffer(
	tb fragmentTB,
	form offerVariantForm,
	reference offerVariantPublication,
	subject offerVariantPublication,
	payload []byte,
	withSupplier bool,
) int64 {
	tb.Helper()

	recorder := &reservedValueRecorder{
		tb:         tb,
		frameCount: offerFrameCount(len(payload), reference.MaxPayloadLength()),
	}
	var supplier aeron.ReservedValueSupplier
	if withSupplier {
		supplier = recorder.supply
	}
	want := reference.OfferWithOffset(payload, 0, len(payload))
	got := form.offer(subject, payload, offerVariantCuts(len(payload)), supplier)
	require.Equal(tb, want, got)
	require.Equal(tb, reference.Position(), subject.Position())

	wantFrames := 0
	if want > 0 {
		wantFrames = recorder.frameCount
	}
	wantCalls := 0
	if withSupplier {
		wantCalls = wantFrames
	}
	require.Len(tb, recorder.frames, wantCalls, "supplier calls")

	terms, dataFrames := expectedTerms(reference, withSupplier)
	require.Equal(tb, wantFrames, dataFrames, "DATA frames")
	for index, term := range terms {
		requireEqualTerm(tb, subject, index, term)
	}
	return got
}

func TestOfferVariants_matchConcatenatedOffer(t *testing.T) {
	supplierNames := map[bool]string{false: "nilSupplier", true: "supplier"}
	for _, kind := range offerVariantKinds {
		probe := kind.openUnlimited()
		maxPayload := probe.MaxPayloadLength()
		maxMessage := probe.MaxMessageLength()
		for _, form := range offerVariantForms {
			for _, length := range offerVariantLengths(maxPayload, maxMessage) {
				for withSupplier, supplierName := range supplierNames {
					name := fmt.Sprintf("%s/%s/%d/%s", kind.name, form.name, length, supplierName)
					t.Run(name, func(t *testing.T) {
						got := requireVariantOffer(
							t,
							form,
							kind.openUnlimited(),
							kind.openUnlimited(),
							patternedPayload(length),
							withSupplier,
						)
						if length > maxMessage {
							require.Equal(t, aeron.UnsupportedMessageLength, got)
						} else {
							require.Positive(t, got)
						}
					})
				}
			}

			t.Run(kind.name+"/"+form.name+"/closed", func(t *testing.T) {
				reference := kind.openUnlimited()
				subject := kind.openUnlimited()
				require.NoError(t, reference.Close())
				require.NoError(t, subject.Close())

				got := requireVariantOffer(t, form, reference, subject, patternedPayload(64), true)
				require.Equal(t, aeron.Closed, got)
			})

			t.Run(kind.name+"/"+form.name+"/backPressured", func(t *testing.T) {
				payload := patternedPayload(3 * maxPayload)
				for _, length := range []int{64, len(payload)} {
					t.Run(fmt.Sprint(length), func(t *testing.T) {
						got := requireVariantOffer(
							t,
							form,
							kind.open(kind.baseConfig(), 0),
							kind.open(kind.baseConfig(), 0),
							payload[:length],
							true,
						)
						require.Equal(t, aeron.BackPressured, got)
					})
				}
			})

			t.Run(kind.name+"/"+form.name+"/termEndPadding", func(t *testing.T) {
				cfg := aeron.LogConfigNearTermEndForTest(kind.baseConfig, 64)
				payload := patternedPayload(maxPayload + 1)
				for _, length := range []int{64, len(payload)} {
					t.Run(fmt.Sprint(length), func(t *testing.T) {
						reference := kind.open(cfg, int64(1)<<62)
						subject := kind.open(cfg, int64(1)<<62)
						got := requireVariantOffer(t, form, reference, subject, payload[:length], true)
						require.Equal(t, aeron.AdminAction, got)
					})
				}
			})

			t.Run(kind.name+"/"+form.name+"/staleReservedValue", func(t *testing.T) {
				reference := kind.openUnlimited()
				subject := kind.openUnlimited()
				for index := range logbuffer.PartitionCount {
					fillStaleTerm(reference.TermBufferForTest(index).Bytes())
					fillStaleTerm(subject.TermBufferForTest(index).Bytes())
				}
				payload := patternedPayload(3 * maxPayload)

				got := requireVariantOffer(t, form, reference, subject, payload, false)
				require.Positive(t, got)

				// Equal terms alone pass when every form writes zero, so
				// each frame must still hold the stale word.
				term := subject.TermBufferForTest(0)
				termOffset := 0
				for frameIndex := range offerFrameCount(len(payload), maxPayload) {
					require.Equal(t, staleReservedValue, logbuffer.FrameReservedValue(term, termOffset),
						"frame %d reserved value", frameIndex)
					termOffset += int(logbuffer.AlignFrameLength(logbuffer.FrameLength(term, termOffset)))
				}
			})
		}

		noParts := map[string][][]byte{"nil": nil, "empty": {}}
		for partsName, parts := range noParts {
			t.Run(kind.name+"/OfferVector_noParts/"+partsName, func(t *testing.T) {
				reference := kind.openUnlimited()
				subject := kind.openUnlimited()

				want := reference.OfferWithOffset(nil, 0, 0)
				got := subject.OfferVector(parts, nil)

				require.Equal(t, want, got)
				require.Positive(t, want)
				require.Equal(t, reference.Position(), subject.Position())
				requireEqualTerms(t, reference, subject)
			})
		}
	}
}

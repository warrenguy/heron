package conductor

import (
	"fmt"

	"github.com/warrenguy/heron/agrona/atomic"
)

func decodeStringASCIIInBody(
	buf atomic.AtomicBuffer,
	bodyOffset, bodyLength, fieldOffset int,
	wrapped error,
) (string, int, error) {
	if fieldOffset > bodyLength {
		return "", 0, fmt.Errorf(
			"%w: fieldOffset=%d bodyLength=%d",
			wrapped,
			fieldOffset,
			bodyLength,
		)
	}
	remaining := bodyLength - fieldOffset
	if remaining < atomic.StringASCIIHeaderLength {
		return "", 0, fmt.Errorf(
			"%w: need=%d got=%d",
			wrapped,
			atomic.StringASCIIHeaderLength,
			remaining,
		)
	}

	stringStart := bodyOffset + fieldOffset
	value, encodedLength, err := atomic.GetStringASCII(
		buf.Slice(stringStart, remaining),
		0,
	)
	if err != nil {
		return "", 0, err
	}
	return value, fieldOffset + encodedLength, nil
}

func encodeStringASCIIInBody(
	buf atomic.AtomicBuffer,
	bodyOffset, fieldOffset int,
	value string,
) (int, error) {
	encodedLength, err := atomic.PutStringASCII(buf, bodyOffset+fieldOffset, value)
	if err != nil {
		return 0, err
	}
	return fieldOffset + encodedLength, nil
}

func fixedPlusTrailingStringBodyLength(fixedLength int, value string) int {
	return fixedLength + atomic.StringASCIIEncodedLength(len(value))
}

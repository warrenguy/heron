package conductor

import (
	"fmt"

	"github.com/warrenguy/heron/atomicbuffer"
)

func decodeStringASCIIInBody(
	buf atomicbuffer.AtomicBuffer,
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
	if remaining < atomicbuffer.StringASCIIHeaderLength {
		return "", 0, fmt.Errorf(
			"%w: need=%d got=%d",
			wrapped,
			atomicbuffer.StringASCIIHeaderLength,
			remaining,
		)
	}

	stringStart := bodyOffset + fieldOffset
	value, encodedLength, err := atomicbuffer.GetStringASCII(
		buf.Slice(stringStart, remaining),
		0,
	)
	if err != nil {
		return "", 0, err
	}
	return value, fieldOffset + encodedLength, nil
}

func encodeStringASCIIInBody(
	buf atomicbuffer.AtomicBuffer,
	bodyOffset, fieldOffset int,
	value string,
) (int, error) {
	encodedLength, err := atomicbuffer.PutStringASCII(buf, bodyOffset+fieldOffset, value)
	if err != nil {
		return 0, err
	}
	return fieldOffset + encodedLength, nil
}

func fixedPlusTrailingStringBodyLength(fixedLength int, value string) int {
	return fixedLength + atomicbuffer.StringASCIIEncodedLength(len(value))
}

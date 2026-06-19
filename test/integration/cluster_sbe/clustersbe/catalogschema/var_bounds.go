package catalogschema

import (
	"errors"
	"fmt"

	"github.com/warrenguy/heron/aeron/sbe"
)

func validateFixtureVarFieldLengths(
	entry FixtureEntry,
	blobs map[string][]byte,
) error {
	keys, ok := templateVarFieldKeys[entry.Template]
	if !ok {
		return nil
	}

	bytesFields := templateVarBytesFieldKeys[entry.Template]
	for _, key := range keys {
		value, present := entry.Fields[key]
		if !present {
			continue
		}
		_, bytesField := bytesFields[key]
		n, err := catalogVarFieldByteLen(value, blobs, bytesField)
		if err != nil {
			return fmt.Errorf("field %q: %w", key, err)
		}
		if n > sbe.ClusterCodecMaxVarLen {
			return fmt.Errorf(
				"field %q: length %d exceeds max %d",
				key, n, sbe.ClusterCodecMaxVarLen,
			)
		}
	}
	return nil
}

func catalogVarFieldByteLen(value any, blobs map[string][]byte, bytesField bool) (int, error) {
	if bytesField {
		data, err := DecodeBytesValue(value, blobs)
		if err != nil {
			return 0, err
		}
		return len(data), nil
	}

	s, ok := value.(string)
	if !ok {
		return 0, errors.New("expected string")
	}
	return len(s), nil
}

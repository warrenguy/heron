package catalogschema

import (
	"encoding/hex"
	"errors"
	"fmt"
	"math"
)

// Hex values must satisfy the same rules as catalog validation (lowercase, even length).
func DecodeBytesValue(
	value any,
	blobs map[string][]byte,
) ([]byte, error) {
	if value == nil {
		return nil, nil
	}

	hexStr, refStr, err := parseBytesCatalogValue(value)
	if err != nil {
		return nil, err
	}
	if refStr != "" {
		data, ok := blobs[refStr]
		if !ok {
			return nil, fmt.Errorf("unknown blob ref %q", refStr)
		}
		return data, nil
	}
	return hex.DecodeString(hexStr)
}

// Numeric values follow Int64FromJSONValue rules (json.Number from ParseJSON).
func HeartbeatFromCatalog(value any) (int64, error) {
	if s, ok := value.(string); ok {
		if s == HeartbeatAbsentSentinel {
			return math.MinInt64, nil
		}
		return 0, fmt.Errorf("unknown heartbeat sentinel %q", s)
	}
	return Int64FromJSONValue(value)
}

func OptionalStringFromCatalog(value any) ([]byte, error) {
	s, ok := value.(string)
	if !ok {
		return nil, errors.New("expected string")
	}
	return []byte(s), nil
}

func bytesRefName(value any) (string, bool) {
	_, refStr, err := parseBytesCatalogValue(value)
	if err != nil || refStr == "" {
		return "", false
	}
	return refStr, true
}

func parseBytesCatalogValue(value any) (hexStr, refStr string, err error) {
	m, ok := value.(map[string]any)
	if !ok {
		return "", "", errors.New("expected hex or ref object")
	}
	hexS, hasHex, hexOK := catalogStringField(m, "hex")
	if hasHex && !hexOK {
		return "", "", errors.New("hex must be string")
	}
	refS, hasRef, _ := catalogStringField(m, "ref")
	return finishBytesParse(hasHex, hasRef, hexS, refS)
}

func catalogStringField(m map[string]any, key string) (s string, present, ok bool) {
	v, present := m[key]
	if !present {
		return "", false, true
	}
	s, ok = v.(string)
	return s, true, ok
}

func finishBytesParse(hasHex, hasRef bool, hexStr, refStr string) (string, string, error) {
	if hasHex == hasRef {
		return "", "", errors.New("expected exactly one of hex or ref")
	}
	if hasHex {
		if err := validateHex(hexStr); err != nil {
			return "", "", err
		}
		return hexStr, "", nil
	}
	if refStr == "" {
		return "", "", errors.New("ref must be non-empty string")
	}
	return "", refStr, nil
}

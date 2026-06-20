package atomic

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestValidateStringASCIIValueByteLen_invalid(t *testing.T) {
	err := validateStringASCIIValueByteLen(math.MaxInt32 + 1)
	require.ErrorIs(t, err, ErrStringASCIILengthInvalid)
}

func TestValidateStringASCIIValue_nonASCII(t *testing.T) {
	err := validateStringASCIIValue("\x80")
	require.ErrorIs(t, err, ErrStringASCIINonASCII)
}

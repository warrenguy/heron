package client_test

import (
	"slices"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/cluster/client"
)

type stubCredentialsSupplier struct {
	encoded      []byte
	challengeRes []byte
}

func (s stubCredentialsSupplier) EncodedCredentials() []byte {
	return s.encoded
}

func (s stubCredentialsSupplier) RespondToChallenge(_ []byte) []byte {
	return s.challengeRes
}

func TestNullCredentialsSupplier_returnsNilSlices(t *testing.T) {
	supplier := client.NullCredentialsSupplier{}

	require.Nil(t, supplier.EncodedCredentials())
	require.Nil(t, supplier.RespondToChallenge([]byte{1, 2, 3}))
}

func TestStubCredentialsSupplier_returnsConfiguredValues(t *testing.T) {
	supplier := stubCredentialsSupplier{
		encoded:      []byte("encoded"),
		challengeRes: []byte("response"),
	}

	require.Equal(t, []byte("encoded"), supplier.EncodedCredentials())
	require.Equal(t, []byte("response"), supplier.RespondToChallenge([]byte("challenge")))
}

func TestStubCredentialsSupplier_doesNotMutateChallengeInput(t *testing.T) {
	challenge := []byte{1, 2, 3, 4}
	original := slices.Clone(challenge)

	supplier := stubCredentialsSupplier{challengeRes: []byte("ok")}
	_ = supplier.RespondToChallenge(challenge)

	require.Equal(t, original, challenge)
}

package client

// CredentialsSupplier provides encoded credentials for archive connect and challenge
// response.
//
// Methods do not return errors. Authentication failure is reported asynchronously on
// the control response stream as ControlResponseCode_ERROR. Nil or empty returned
// slices encode as zero-length wire fields.
//
// Hot-path suppliers may return reused buffers when possible.
type CredentialsSupplier interface {
	// EncodedCredentials returns credentials for AuthConnectRequest.encodedCredentials.
	// Nil or empty slice means no credentials on the wire.
	//
	// The client does not retain the returned slice beyond connect encoding; callers
	// must not rely on the client keeping the buffer alive after the connect callback.
	EncodedCredentials() []byte

	// RespondToChallenge returns credentials for ChallengeResponse.encodedCredentials.
	//
	// encodedChallenge is valid only for the duration of this call; implementations
	// must not mutate it. Copy the slice if it must be retained after the call
	// returns.
	RespondToChallenge(encodedChallenge []byte) []byte
}

// NullCredentialsSupplier is the default when none is configured.
type NullCredentialsSupplier struct{}

func (NullCredentialsSupplier) EncodedCredentials() []byte { return nil }

func (NullCredentialsSupplier) RespondToChallenge([]byte) []byte { return nil }

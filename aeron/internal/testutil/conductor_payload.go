package testutil

import (
	"luas.fast/aeron/conductor"
	"luas.fast/atomicbuffer"
)

func ClientTimeoutPayload(clientID int64) []byte {
	payload := make([]byte, conductor.ClientTimeoutBodyLength)
	atomicbuffer.Wrap(payload).PutInt64(0, clientID)
	return payload
}

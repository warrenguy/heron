package testutil

import (
	"github.com/warrenguy/luas/aeron/conductor"
	"github.com/warrenguy/luas/atomicbuffer"
)

func ClientTimeoutPayload(clientID int64) []byte {
	payload := make([]byte, conductor.ClientTimeoutBodyLength)
	atomicbuffer.Wrap(payload).PutInt64(0, clientID)
	return payload
}

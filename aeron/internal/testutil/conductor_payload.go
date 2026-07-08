package testutil

import (
	"github.com/warrenguy/heron/aeron/conductor"
	"github.com/warrenguy/heron/atomicbuffer"
)

func ClientTimeoutPayload(clientID int64) []byte {
	payload := make([]byte, conductor.ClientTimeoutBodyLength)
	atomicbuffer.Wrap(payload).PutInt64(0, clientID)
	return payload
}

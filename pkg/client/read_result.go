package client

import (
	messages "github.com/eventstore/EventStore-Client-Go/pkg/messages"
	api "github.com/eventstore/EventStore-Client-Go/protos"
)

// ReadResult ...
type ReadResult struct {
	Events []messages.RecordedEvent
}

// ReadResultFromReadRespo ...
func ReadResultFromReadRespo(readResponse *api.ReadResp) *ReadResult {
	return &ReadResult{}
}

package types

import (
	"encoding/json"

	uuid "github.com/gofrs/uuid"
)

// ProposedEvent ...
type ProposedEvent struct {
	eventID     uuid.UUID
	eventType   string
	contentType string
	data        []byte
	metadata    []byte
}

func (e *ProposedEvent) EventID() uuid.UUID {
	return e.eventID
}

func (e *ProposedEvent) EventType() string {
	return e.eventType
}

func (e *ProposedEvent) ContentType() string {
	return e.contentType
}

func (e *ProposedEvent) Data() []byte {
	return e.data
}

func (e *ProposedEvent) Metadata() []byte {
	return e.metadata
}

func (e *ProposedEvent) SetEventID(value uuid.UUID) {
	e.eventID = value
}

func (e *ProposedEvent) SetEventType(value string) {
	e.eventType = value
}

func (e *ProposedEvent) SetContentType(value string) {
	e.contentType = value
}

func (e *ProposedEvent) SetJsonData(payload interface{}) error {
	bytes, err := json.Marshal(payload)

	if err != nil {
		return err
	}

	e.data = bytes
	e.contentType = "application/json"

	return nil
}

func (e *ProposedEvent) SetBinaryData(payload []byte) {
	e.data = payload
	e.contentType = "application/octet-stream"
}

func (e *ProposedEvent) SetData(payload []byte) {
	e.data = payload
}

func (e *ProposedEvent) SetMetadata(payload []byte) {
	e.metadata = payload
}

package messages

import (
	"encoding/json"

	uuid "github.com/gofrs/uuid"
)

// ProposedEvent ...
type ProposedEvent struct {
	eventID      uuid.UUID
	eventType    string
	contentType  string
	data         []byte
	metadata []byte
}

func NewJsonProposedEvent(eventType string, payload interface{}) (ProposedEvent, error) {
	event := ProposedEvent {}
	bytes, err := json.Marshal(payload)

	if err != nil {
		return event, err
	}

	event.eventType = eventType
	event.contentType = "application/json"
	event.data = bytes
	event.metadata = []byte{}

	return event, nil
}

func NewBinaryProposedEvent(eventType string, bytes []byte) ProposedEvent {
	event := ProposedEvent {
		eventType: eventType,
		contentType: "application/octet-stream",
		data: bytes,
		metadata: []byte{},
	}

	return event
}

func (event ProposedEvent) GetEventID() uuid.UUID {
	return event.eventID
}

func (event ProposedEvent) GetEventType() string {
	return event.eventType
}

func (event ProposedEvent) GetContentType() string {
	return event.contentType
}

func (event ProposedEvent) GetData() []byte {
	return event.data
}

func (event ProposedEvent) GetMetadata() []byte {
	return event.metadata
}

func (event ProposedEvent) EventID(value uuid.UUID) ProposedEvent {
	event.eventID = value
	return event
}

func (event ProposedEvent) EventType(value string) ProposedEvent {
	event.eventType = value
	return event
}

func (event ProposedEvent) ContentType(value string) ProposedEvent {
	event.contentType = value
	return event
}

func (event ProposedEvent) JsonData(payload interface{}) (ProposedEvent, error) {
	bytes, err := json.Marshal(payload)

	if err != nil {
		return event, err
	}

	event.data = bytes
	event.contentType = "application/json"

	return event, nil
}

func (event ProposedEvent) BinaryData(payload []byte) ProposedEvent {
	event.data = payload
	event.contentType = "application/octet-stream"

	return event
}

func (event ProposedEvent) Data(payload []byte) ProposedEvent {
	event.data = payload
	return event
}

func (event ProposedEvent) Metadata(payload []byte) ProposedEvent {
	event.metadata = payload
	return event
}

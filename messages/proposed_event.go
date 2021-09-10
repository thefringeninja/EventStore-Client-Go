package messages

import (
	"encoding/json"

	uuid "github.com/gofrs/uuid"
)

// ProposedEvent ...
type ProposedEvent struct {
	EventID      uuid.UUID
	EventType    string
	ContentType  string
	Data         []byte
	UserMetadata []byte
}

type Builder struct {
	EventID      uuid.UUID
	EventType    string
	ContentType  string
	Data         []byte
	UserMetadata []byte
}

func NewJsonEvent(eventType string, payload interface{}) (*Builder, error) {
	bytes, err := json.Marshal(payload)

	if err != nil {
		return nil, err
	}

	return NewEvent(eventType, "application/json", bytes), nil
}

func NewBinaryEvent(eventType string, payload []byte) *Builder {
	return NewEvent(eventType, "application/octet-stream", payload)
}

func NewEvent(eventType string, contentType string, payload []byte) *Builder {
	return &Builder{
		EventID:      uuid.Nil,
		EventType:    eventType,
		ContentType:  contentType,
		Data:         payload,
		UserMetadata: []byte{},
	}
}

func (builder *Builder) SetMetadataAsJson(payload interface{}) error {
	bytes, err := json.Marshal(payload)

	if err != nil {
		return err
	}

	builder.UserMetadata = bytes

	return nil
}

func (builder *Builder) SetMetadata(payload []byte) *Builder {
	builder.UserMetadata = payload

	return builder
}

func (builder *Builder) SetEventID(id uuid.UUID) *Builder {
	builder.EventID = id

	return builder
}

func (builder *Builder) Build() ProposedEvent {
	var eventId uuid.UUID

	if builder.EventID == uuid.Nil {
		eventId = uuid.Must(uuid.NewV4())
	} else {
		eventId = builder.EventID
	}

	return ProposedEvent{
		EventID:      eventId,
		EventType:    builder.EventType,
		ContentType:  builder.ContentType,
		Data:         builder.Data,
		UserMetadata: builder.UserMetadata,
	}
}

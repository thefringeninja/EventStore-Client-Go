package messages

import (
	"time"

	"github.com/EventStore/EventStore-Client-Go/types"
	uuid "github.com/gofrs/uuid"
)

// RecordedEvent ...
type RecordedEvent struct {
	EventID        uuid.UUID
	EventType      string
	ContentType    string
	StreamID       string
	EventNumber    uint64
	Position       types.Position
	CreatedDate    time.Time
	Data           []byte
	SystemMetadata map[string]string
	UserMetadata   []byte
}

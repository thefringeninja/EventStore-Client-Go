package subscription

import (
	"github.com/EventStore/EventStore-Client-Go/messages"
	"github.com/EventStore/EventStore-Client-Go/position"
)

type Event struct {
	EventAppeared       *messages.ResolvedEvent
	SubscriptionDropped *Dropped
	CheckPointReached   *position.Position
}

type Dropped struct {
	Error error
}

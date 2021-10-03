package subscription

import (
	"github.com/EventStore/EventStore-Client-Go/messages"
	"github.com/EventStore/EventStore-Client-Go/types"
)

type Event struct {
	EventAppeared       *messages.ResolvedEvent
	SubscriptionDropped *Dropped
	CheckPointReached   *types.Position
}

type Dropped struct {
	Error error
}

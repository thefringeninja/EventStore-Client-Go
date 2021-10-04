package client

import (
	"github.com/EventStore/EventStore-Client-Go/types"
)

type ReadStreamEventsOptions struct {
	Direction      types.Direction
	From           types.StreamPosition
	ResolveLinkTos bool
}

func (o *ReadStreamEventsOptions) setDefaults() {
	if o.From == nil {
		o.From = types.Start{}
	}
}

type ReadAllEventsOptions struct {
	Direction      types.Direction
	From           types.AllPosition
	ResolveLinkTos bool
}

func (o *ReadAllEventsOptions) setDefaults() {
	if o.From == nil {
		o.From = types.Start{}
	}
}

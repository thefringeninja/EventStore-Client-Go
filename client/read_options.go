package client

import (
	"github.com/EventStore/EventStore-Client-Go/types"
)

type ReadStreamEventsOptions struct {
	Direction      types.Direction
	From           types.StreamPosition
	ResolveLinkTos bool
	Authenticated  *types.Credentials
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
	Authenticated  *types.Credentials
}

func (o *ReadAllEventsOptions) setDefaults() {
	if o.From == nil {
		o.From = types.Start{}
	}
}

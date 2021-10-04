package client

import (
	"github.com/EventStore/EventStore-Client-Go/types"
)

type ReadStreamOptions struct {
	Direction      types.Direction
	From           types.StreamPosition
	ResolveLinkTos bool
	Authenticated  *types.Credentials
}

func (o *ReadStreamOptions) setDefaults() {
	if o.From == nil {
		o.From = types.Start{}
	}
}

type ReadAllOptions struct {
	Direction      types.Direction
	From           types.AllPosition
	ResolveLinkTos bool
	Authenticated  *types.Credentials
}

func (o *ReadAllOptions) setDefaults() {
	if o.From == nil {
		o.From = types.Start{}
	}
}

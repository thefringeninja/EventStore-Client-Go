package client

import (
	"github.com/EventStore/EventStore-Client-Go/types"
)

type SubscribeToStreamOptions struct {
	From         types.StreamPosition
	ResolveLinks bool
}

func (o *SubscribeToStreamOptions) setDefaults() {
	if o.From == nil {
		o.From = types.End{}
	}
}

type SubscribeToAllOptions struct {
	From               types.AllPosition
	ResolveLinks       bool
	MaxSearchWindow    int
	CheckpointInterval int
	Filter             *types.SubscriptionFilter
}

func (o *SubscribeToAllOptions) setDefaults() {
	if o.From == nil {
		o.From = types.End{}
	}

	if o.Filter != nil {
		if o.MaxSearchWindow == 0 {
			o.MaxSearchWindow = 32
		}

		if o.CheckpointInterval == 0 {
			o.CheckpointInterval = 1
		}
	}
}

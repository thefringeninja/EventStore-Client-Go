package client

import (
	"github.com/EventStore/EventStore-Client-Go/client/filtering"
	"github.com/EventStore/EventStore-Client-Go/position"
	"github.com/EventStore/EventStore-Client-Go/stream"
)

type SubscribeToStreamOptions struct {
	position     stream.StreamPosition
	resolveLinks bool
}

func (o *SubscribeToStreamOptions) setDefaults() {
	if o.position == nil {
		o.position = stream.End()
	}
}

func (o *SubscribeToStreamOptions) SetFrom(value stream.StreamPosition) {
	o.position = value
}

func (o *SubscribeToStreamOptions) SetFromStart() {
	o.position = stream.Start()
}

func (o *SubscribeToStreamOptions) SetFromEnd() {
	o.position = stream.End()
}

func (o *SubscribeToStreamOptions) SetFromRevision(value uint64) {
	o.position = stream.Revision(value)
}

func (o *SubscribeToStreamOptions) SetResolveLinks() {
	o.resolveLinks = true
}

func (o *SubscribeToStreamOptions) Position() stream.StreamPosition {
	return o.position
}

func (o *SubscribeToStreamOptions) ResolveLinks() bool {
	return o.resolveLinks
}

type SubscribeToAllOptions struct {
	position     stream.AllStreamPosition
	resolveLinks bool
	filter       []filtering.SubscriptionFilterOptions
}

func (o *SubscribeToAllOptions) setDefaults() {
	if o.position == nil {
		o.position = stream.End()
	}

	if o.filter == nil {
		o.filter = []filtering.SubscriptionFilterOptions{}
	}
}

func (o *SubscribeToAllOptions) SetFrom(value stream.AllStreamPosition) {
	o.position = value
}

func (o *SubscribeToAllOptions) SetFromStart() {
	o.position = stream.Start()
}

func (o *SubscribeToAllOptions) SetFromEnd() {
	o.position = stream.End()
}

func (o *SubscribeToAllOptions) SetFromPosition(value position.Position) {
	o.position = stream.Position(value)
}

func (o *SubscribeToAllOptions) SetResolveLinks() {
	o.resolveLinks = true
}

func (o *SubscribeToAllOptions) SetFilter(value filtering.SubscriptionFilterOptions) {
	o.filter = []filtering.SubscriptionFilterOptions{value}
}

func (o *SubscribeToAllOptions) Position() stream.AllStreamPosition {
	return o.position
}

func (o *SubscribeToAllOptions) ResolveLinks() bool {
	return o.resolveLinks
}

func (o *SubscribeToAllOptions) Filter() *filtering.SubscriptionFilterOptions {
	if len(o.filter) == 0 {
		return nil
	}

	return &o.filter[0]
}

package client

import (
	"github.com/EventStore/EventStore-Client-Go/client/filtering"
	"github.com/EventStore/EventStore-Client-Go/position"
	"github.com/EventStore/EventStore-Client-Go/stream_position"
)

type SubscribeToStreamOptions struct {
	position     stream_position.StreamPosition
	resolveLinks bool
}

func (o *SubscribeToStreamOptions) SetDefaults() {
	o.position = stream_position.End()
	o.resolveLinks = false
}

func (o *SubscribeToStreamOptions) SetFrom(value stream_position.StreamPosition) {
	o.position = value
}

func (o *SubscribeToStreamOptions) SetFromStart() {
	o.position = stream_position.Start()
}

func (o *SubscribeToStreamOptions) SetFromEnd() {
	o.position = stream_position.End()
}

func (o *SubscribeToStreamOptions) SetFromRevision(value uint64) {
	o.position = stream_position.Revision(value)
}

func (o *SubscribeToStreamOptions) SetResolveLinks() {
	o.resolveLinks = true
}

func (o *SubscribeToStreamOptions) Position() stream_position.StreamPosition {
	return o.position
}

func (o *SubscribeToStreamOptions) ResolveLinks() bool {
	return o.resolveLinks
}

type SubscribeToAllOptions struct {
	position     stream_position.AllStreamPosition
	resolveLinks bool
	filter       []filtering.SubscriptionFilterOptions
}

func (o *SubscribeToAllOptions) SetDefaults() {
	o.position = stream_position.End()
	o.resolveLinks = false
	o.filter = []filtering.SubscriptionFilterOptions{}
}

func (o *SubscribeToAllOptions) SetFrom(value stream_position.AllStreamPosition) {
	o.position = value
}

func (o *SubscribeToAllOptions) SetFromStart() {
	o.position = stream_position.Start()
}

func (o *SubscribeToAllOptions) SetFromEnd() {
	o.position = stream_position.End()
}

func (o *SubscribeToAllOptions) SetFromPosition(value position.Position) {
	o.position = stream_position.Position(value)
}

func (o *SubscribeToAllOptions) SetResolveLinks() {
	o.resolveLinks = true
}

func (o *SubscribeToAllOptions) SetFilter(value filtering.SubscriptionFilterOptions) {
	o.filter = []filtering.SubscriptionFilterOptions{value}
}

func (o *SubscribeToAllOptions) Position() stream_position.AllStreamPosition {
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

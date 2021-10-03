package client

import (
	"github.com/EventStore/EventStore-Client-Go/client/filtering"
	"github.com/EventStore/EventStore-Client-Go/stream"
	"github.com/EventStore/EventStore-Client-Go/types"
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
	position           stream.AllStreamPosition
	resolveLinks       bool
	maxSearchWindow    int
	checkpointInterval int
	filter             []filtering.SubscriptionFilter
}

func (o *SubscribeToAllOptions) setDefaults() {
	if o.position == nil {
		o.position = stream.End()
	}

	if len(o.filter) != 0 {
		if o.maxSearchWindow == 0 {
			o.maxSearchWindow = 32
		}

		if o.checkpointInterval == 0 {
			o.checkpointInterval = 1
		}
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

func (o *SubscribeToAllOptions) SetFromPosition(value types.Position) {
	o.position = stream.Position(value)
}

func (o *SubscribeToAllOptions) SetResolveLinks() {
	o.resolveLinks = true
}

func (o *SubscribeToAllOptions) SetFilter(value filtering.SubscriptionFilter) {
	o.filter = []filtering.SubscriptionFilter{value}
}

func (o *SubscribeToAllOptions) Position() stream.AllStreamPosition {
	return o.position
}

func (o *SubscribeToAllOptions) ResolveLinks() bool {
	return o.resolveLinks
}

func (o *SubscribeToAllOptions) MaxSearchWindow() int {
	return o.maxSearchWindow
}

func (o *SubscribeToAllOptions) SetNoMaxSearchWindow() {
	o.maxSearchWindow = -1
}

func (o *SubscribeToAllOptions) CheckpointInterval() int {
	return o.checkpointInterval
}

func (o *SubscribeToAllOptions) SetMaxSearchWindow(value int) {
	o.maxSearchWindow = value
}

func (o *SubscribeToAllOptions) SetCheckpointInterval(value int) {
	o.checkpointInterval = value
}

func (o *SubscribeToAllOptions) Filter() *filtering.SubscriptionFilter {
	if len(o.filter) == 0 {
		return nil
	}

	return &o.filter[0]
}

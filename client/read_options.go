package client

import (
	"github.com/EventStore/EventStore-Client-Go/direction"
	"github.com/EventStore/EventStore-Client-Go/position"
	"github.com/EventStore/EventStore-Client-Go/stream"
)

type ReadStreamEventsOptions struct {
	direction    direction.Direction
	position     stream.StreamPosition
	resolveLinks bool
}

func (o *ReadStreamEventsOptions) setDefaults() {
	if o.position == nil {
		o.position = stream.Start()
	}
}

func (o *ReadStreamEventsOptions) SetForwards() {
	o.direction = direction.Forwards
}

func (o *ReadStreamEventsOptions) SetBackwards() {
	o.direction = direction.Backwards
}

func (o *ReadStreamEventsOptions) SetDirection(dir direction.Direction) {
	o.direction = dir
}

func (o *ReadStreamEventsOptions) SetFrom(position stream.StreamPosition) {
	o.position = position
}

func (o *ReadStreamEventsOptions) SetFromStart() {
	o.position = stream.RevisionStart{}
}

func (o *ReadStreamEventsOptions) SetFromEnd() {
	o.position = stream.RevisionEnd{}
}

func (o *ReadStreamEventsOptions) SetFromRevision(value uint64) {
	o.position = stream.RevisionExact{Value: value}
}

func (o *ReadStreamEventsOptions) SetResolveLinks() {
	o.resolveLinks = true
}

func (o *ReadStreamEventsOptions) Direction() direction.Direction {
	return o.direction
}

func (o *ReadStreamEventsOptions) Position() stream.StreamPosition {
	return o.position
}

func (o *ReadStreamEventsOptions) ResolveLinks() bool {
	return o.resolveLinks
}

type ReadAllEventsOptions struct {
	direction    direction.Direction
	position     stream.AllStreamPosition
	resolveLinks bool
}

func (o *ReadAllEventsOptions) setDefaults() {
	if o.position == nil {
		o.position = stream.Start()
	}
}

func (o *ReadAllEventsOptions) SetForwards() {
	o.direction = direction.Forwards
}

func (o *ReadAllEventsOptions) SetBackwards() {
	o.direction = direction.Backwards
}

func (o *ReadAllEventsOptions) SetDirection(dir direction.Direction) {
	o.direction = dir
}

func (o *ReadAllEventsOptions) SetFromStart() {
	o.position = stream.RevisionStart{}
}

func (o *ReadAllEventsOptions) SetFromEnd() {
	o.position = stream.RevisionEnd{}
}

func (o *ReadAllEventsOptions) SetFromPosition(pos position.Position) {
	o.position = stream.Position(pos)
}

func (o *ReadAllEventsOptions) SetFrom(position stream.AllStreamPosition) {
	o.position = position
}

func (o *ReadAllEventsOptions) SetResolveLinks() {
	o.resolveLinks = true
}

func (o *ReadAllEventsOptions) Direction() direction.Direction {
	return o.direction
}

func (o *ReadAllEventsOptions) Position() stream.AllStreamPosition {
	return o.position
}

func (o *ReadAllEventsOptions) ResolveLinks() bool {
	return o.resolveLinks
}

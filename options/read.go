package options

import (
	"github.com/EventStore/EventStore-Client-Go/direction"
	"github.com/EventStore/EventStore-Client-Go/position"
	"github.com/EventStore/EventStore-Client-Go/stream_position"
)

type ReadStreamEventsOptions struct {
	direction    direction.Direction
	position     stream_position.StreamPosition
	resolveLinks bool
}

func (o *ReadStreamEventsOptions) SetDefaults() {
	o.direction = direction.Forwards
	o.position = stream_position.RevisionStart{}
	o.resolveLinks = false
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

func (o *ReadStreamEventsOptions) SetFrom(position stream_position.StreamPosition) {
	o.position = position
}

func (o *ReadStreamEventsOptions) SetFromStart() {
	o.position = stream_position.RevisionStart{}
}

func (o *ReadStreamEventsOptions) SetFromEnd() {
	o.position = stream_position.RevisionEnd{}
}

func (o *ReadStreamEventsOptions) SetFromRevision(value uint64) {
	o.position = stream_position.RevisionExact{Value: value}
}

func (o *ReadStreamEventsOptions) SetResolveLinks() {
	o.resolveLinks = true
}

func (o *ReadStreamEventsOptions) Direction() direction.Direction {
	return o.direction
}

func (o *ReadStreamEventsOptions) Position() stream_position.StreamPosition {
	return o.position
}

func (o *ReadStreamEventsOptions) ResolveLinks() bool {
	return o.resolveLinks
}

type ReadAllEventsOptions struct {
	direction    direction.Direction
	position     stream_position.AllStreamPosition
	resolveLinks bool
}

func (o *ReadAllEventsOptions) SetDefaults() {
	o.direction = direction.Forwards
	o.position = stream_position.RevisionStart{}
	o.resolveLinks = false
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
	o.position = stream_position.RevisionStart{}
}

func (o *ReadAllEventsOptions) SetFromEnd() {
	o.position = stream_position.RevisionEnd{}
}

func (o *ReadAllEventsOptions) SetFromPosition(pos position.Position) {
	o.position = stream_position.Position(pos)
}

func (o *ReadAllEventsOptions) SetFrom(position stream_position.AllStreamPosition) {
	o.position = position
}

func (o *ReadAllEventsOptions) SetResolveLinks() {
	o.resolveLinks = true
}

func (o *ReadAllEventsOptions) Direction() direction.Direction {
	return o.direction
}

func (o *ReadAllEventsOptions) Position() stream_position.AllStreamPosition {
	return o.position
}

func (o *ReadAllEventsOptions) ResolveLinks() bool {
	return o.resolveLinks
}

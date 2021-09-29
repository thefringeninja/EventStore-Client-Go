package options

import (
	"github.com/EventStore/EventStore-Client-Go/client/filtering"
	"github.com/EventStore/EventStore-Client-Go/persistent"
	"github.com/EventStore/EventStore-Client-Go/position"
	"github.com/EventStore/EventStore-Client-Go/stream_position"
)

type PersistentStreamSubscriptionOptions struct {
	settings persistent.SubscriptionSettings
	position stream_position.StreamPosition
}

func (o *PersistentStreamSubscriptionOptions) SetDefaults() {
	o.settings = persistent.SubscriptionSettingsDefault()
	o.position = stream_position.End()
}

func (o *PersistentStreamSubscriptionOptions) SetSettings(settings persistent.SubscriptionSettings) {
	o.settings = settings
}

func (o *PersistentStreamSubscriptionOptions) SetFrom(position stream_position.StreamPosition) {
	o.position = position
}

func (o *PersistentStreamSubscriptionOptions) SetFromStart() {
	o.position = stream_position.Start()
}

func (o *PersistentStreamSubscriptionOptions) SetFromEnd() {
	o.position = stream_position.End()
}

func (o *PersistentStreamSubscriptionOptions) SetFromRevision(value uint64) {
	o.position = stream_position.Revision(value)
}

func (o *PersistentStreamSubscriptionOptions) Settings() persistent.SubscriptionSettings {
	return o.settings
}

func (o *PersistentStreamSubscriptionOptions) From() stream_position.StreamPosition {
	return o.position
}

type PersistentAllSubscriptionOptions struct {
	settings persistent.SubscriptionSettings
	position stream_position.AllStreamPosition
	filter   []filtering.SubscriptionFilterOptions
}

func (o *PersistentAllSubscriptionOptions) SetDefaults() {
	o.settings = persistent.SubscriptionSettingsDefault()
	o.position = stream_position.End()
	o.filter = []filtering.SubscriptionFilterOptions{}
}

func (o *PersistentAllSubscriptionOptions) SetSettings(settings persistent.SubscriptionSettings) {
	o.settings = settings
}

func (o *PersistentAllSubscriptionOptions) SetFrom(position stream_position.AllStreamPosition) {
	o.position = position
}

func (o *PersistentAllSubscriptionOptions) SetFromStart() {
	o.position = stream_position.Start()
}

func (o *PersistentAllSubscriptionOptions) SetFromEnd() {
	o.position = stream_position.End()
}

func (o *PersistentAllSubscriptionOptions) SetFromPosition(value position.Position) {
	o.position = stream_position.Position(value)
}

func (o *PersistentAllSubscriptionOptions) SetFilter(filter filtering.SubscriptionFilterOptions) {
	o.filter = []filtering.SubscriptionFilterOptions{filter}
}

func (o *PersistentAllSubscriptionOptions) Settings() persistent.SubscriptionSettings {
	return o.settings
}

func (o *PersistentAllSubscriptionOptions) From() stream_position.AllStreamPosition {
	return o.position
}

func (o *PersistentAllSubscriptionOptions) Filter() *filtering.SubscriptionFilterOptions {
	if len(o.filter) == 0 {
		return nil
	}

	return &o.filter[0]
}

type ConnectToPersistentSubscriptionOptions struct {
	batchSize int32
}

func (o *ConnectToPersistentSubscriptionOptions) SetDefaults() {
	o.batchSize = 10
}

func (o *ConnectToPersistentSubscriptionOptions) SetBatchSize(value int32) {
	o.batchSize = value
}

func (o *ConnectToPersistentSubscriptionOptions) BatchSize() int32 {
	return o.batchSize
}

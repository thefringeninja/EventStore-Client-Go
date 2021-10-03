package client

import "github.com/EventStore/EventStore-Client-Go/types"

type TombstoneStreamOptions struct {
	expectedRevision types.ExpectedRevision
}

func (o *TombstoneStreamOptions) setDefaults() {
	if o.expectedRevision == nil {
		o.expectedRevision = types.Any{}
	}
}

func (o *TombstoneStreamOptions) SetExpectedRevision(revision types.ExpectedRevision) {
	o.expectedRevision = revision
}

func (o *TombstoneStreamOptions) ExpectedRevision() types.ExpectedRevision {
	return o.expectedRevision
}

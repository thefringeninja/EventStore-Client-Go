package client

import (
	"github.com/EventStore/EventStore-Client-Go/types"
)

type DeleteStreamOptions struct {
	expectedRevision types.ExpectedRevision
}

func (o *DeleteStreamOptions) setDefaults() {
	if o.expectedRevision == nil {
		o.expectedRevision = types.Any{}
	}
}

func (o *DeleteStreamOptions) SetExpectedRevision(revision types.ExpectedRevision) {
	o.expectedRevision = revision
}

func (o *DeleteStreamOptions) ExpectedRevision() types.ExpectedRevision {
	return o.expectedRevision
}

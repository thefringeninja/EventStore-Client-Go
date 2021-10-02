package client

import (
	"github.com/EventStore/EventStore-Client-Go/stream"
)

type DeleteStreamOptions struct {
	expectedRevision stream.ExpectedRevision
}

func (o *DeleteStreamOptions) setDefaults() {
	if o.expectedRevision == nil {
		o.expectedRevision = stream.Any()
	}
}

func (o *DeleteStreamOptions) SetExpectedRevision(revision stream.ExpectedRevision) {
	o.expectedRevision = revision
}

func (o *DeleteStreamOptions) SetExpectNoStream() {
	o.expectedRevision = stream.NoStream()
}

func (o *DeleteStreamOptions) SetExpectStreamExists() {
	o.expectedRevision = stream.Exists()
}

func (o *DeleteStreamOptions) SetExpectRevision(value uint64) {
	o.expectedRevision = stream.Exact(value)
}

func (o *DeleteStreamOptions) ExpectedRevision() stream.ExpectedRevision {
	return o.expectedRevision
}

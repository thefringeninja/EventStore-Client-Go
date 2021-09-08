package options

import (
	stream_revision "github.com/EventStore/EventStore-Client-Go/streamrevision"
)

type DeleteStreamOptions struct {
	Revision stream_revision.ExpectedRevision
}

func DeleteStreamOptionsDefault() *DeleteStreamOptions {
	return &DeleteStreamOptions {
		Revision: stream_revision.Any(),
	}
}

func (options *DeleteStreamOptions) ExpectedRevision(revision stream_revision.ExpectedRevision) *DeleteStreamOptions {
	options.Revision = revision

	return options
}

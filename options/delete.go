package options

import (
	stream_revision "github.com/EventStore/EventStore-Client-Go/streamrevision"
)

type DeleteStreamOptions struct {
	expectedRevision stream_revision.ExpectedRevision
}

func DeleteStreamOptionsDefault() DeleteStreamOptions {
	return DeleteStreamOptions {
		expectedRevision: stream_revision.Any(),
	}
}

func (options DeleteStreamOptions) ExpectedRevision(revision stream_revision.ExpectedRevision) DeleteStreamOptions {
	options.expectedRevision = revision

	return options
}

func (options DeleteStreamOptions) GetExpectedRevision() stream_revision.ExpectedRevision {
	return options.expectedRevision
}

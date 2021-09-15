package options

import (
	stream_revision "github.com/EventStore/EventStore-Client-Go/streamrevision"
)

type TombstoneStreamOptions struct {
	expectedRevision stream_revision.ExpectedRevision
}

func TombstoneStreamOptionsDefault() TombstoneStreamOptions {
	return TombstoneStreamOptions {
		expectedRevision: stream_revision.Any(),
	}
}

func (options TombstoneStreamOptions) ExpectedRevision(revision stream_revision.ExpectedRevision) TombstoneStreamOptions {
	options.expectedRevision = revision

	return options
}

func (options TombstoneStreamOptions) GetExpectedRevision() stream_revision.ExpectedRevision {
	return options.expectedRevision
}

package client

import api "github.com/eventstore/EventStore-Client-Go/protos"

// AppendResult ...
type AppendResult struct {
	CommitPosition  uint64
	PreparePosition uint64
	Revision        uint64
}

// AppendResultFromAppendResp ...
func AppendResultFromAppendResp(appendResponse *api.AppendResp) *AppendResult {
	position := appendResponse.GetPosition()
	if position != nil {
		return &AppendResult{
			CommitPosition:  position.GetCommitPosition(),
			PreparePosition: position.GetPreparePosition(),
			Revision:        appendResponse.GetCurrentRevision(),
		}
	}

	return &AppendResult{
		CommitPosition:  0,
		PreparePosition: 0,
		Revision:        appendResponse.GetCurrentRevision(),
	}
}

package client

import "github.com/EventStore/EventStore-Client-Go/types"

type TombstoneStreamOptions struct {
	ExpectedRevision types.ExpectedRevision
	Authenticated    *types.Credentials
}

func (o *TombstoneStreamOptions) setDefaults() {
	if o.ExpectedRevision == nil {
		o.ExpectedRevision = types.Any{}
	}
}

package persistent

import (
	"context"
)

type SyncReadConnectionFactory interface {
	NewSyncReadConnection(client protoClient,
		subscriptionId string,
		cancel context.CancelFunc,
	) SyncReadConnection
}

type SyncReadConnectionFactoryImpl struct{}

func (factory SyncReadConnectionFactoryImpl) NewSyncReadConnection(
	client protoClient,
	subscriptionId string,
	cancel context.CancelFunc) SyncReadConnection {

	return newSyncReadConnection(client, subscriptionId, cancel)
}

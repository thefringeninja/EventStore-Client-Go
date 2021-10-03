package client

import (
	"context"

	"github.com/EventStore/EventStore-Client-Go/internal/protoutils"
	"github.com/EventStore/EventStore-Client-Go/stream"
	"github.com/EventStore/EventStore-Client-Go/types"

	"github.com/EventStore/EventStore-Client-Go/client/filtering"
	"github.com/EventStore/EventStore-Client-Go/protos/persistent"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

type persistentClient struct {
	inner                        *grpcClient
	persistentSubscriptionClient persistent.PersistentSubscriptionsClient
}

func (client *persistentClient) ConnectToPersistentSubscription(
	ctx context.Context,
	handle connectionHandle,
	bufferSize int32,
	streamName string,
	groupName string,
) (*PersistentSubscription, error) {
	var headers, trailers metadata.MD
	ctx, cancel := context.WithCancel(ctx)
	readClient, err := client.persistentSubscriptionClient.Read(ctx, grpc.Header(&headers), grpc.Trailer(&trailers))
	if err != nil {
		defer cancel()
		err = client.inner.handleError(handle, headers, trailers, err)
		return nil, types.PersistentSubscriptionFailedToInitClientError(err)
	}

	err = readClient.Send(protoutils.ToPersistentReadRequest(bufferSize, groupName, []byte(streamName)))
	if err != nil {
		defer cancel()
		return nil, types.PersistentSubscriptionFailedSendStreamInitError(err)
	}

	readResult, err := readClient.Recv()
	if err != nil {
		defer cancel()
		return nil, types.PersistentSubscriptionFailedReceiveStreamInitError(err)
	}
	switch readResult.Content.(type) {
	case *persistent.ReadResp_SubscriptionConfirmation_:
		{
			asyncConnection := NewPersistentSubscription(
				readClient,
				readResult.GetSubscriptionConfirmation().SubscriptionId,
				cancel)

			return asyncConnection, nil
		}
	}

	defer cancel()
	return nil, types.PersistentSubscriptionNoConfirmationError(err)
}

func (client *persistentClient) CreateStreamSubscription(
	ctx context.Context,
	handle connectionHandle,
	streamName string,
	groupName string,
	position stream.StreamPosition,
	settings types.SubscriptionSettings,
) error {
	createSubscriptionConfig := protoutils.CreatePersistentRequestProto(streamName, groupName, position, settings)
	var headers, trailers metadata.MD
	_, err := client.persistentSubscriptionClient.Create(ctx, createSubscriptionConfig, grpc.Header(&headers), grpc.Trailer(&trailers))
	if err != nil {
		err = client.inner.handleError(handle, headers, trailers, err)
		return types.PersistentSubscriptionFailedCreationError(err)
	}

	return nil
}

func (client *persistentClient) CreateAllSubscription(
	ctx context.Context,
	handle connectionHandle,
	groupName string,
	position stream.AllStreamPosition,
	settings types.SubscriptionSettings,
	filter *filtering.SubscriptionFilterOptions,
) error {
	protoConfig, err := protoutils.CreatePersistentRequestAllOptionsProto(groupName, position, settings, filter)
	if err != nil {
		return err
	}

	var headers, trailers metadata.MD
	_, err = client.persistentSubscriptionClient.Create(ctx, protoConfig, grpc.Header(&headers), grpc.Trailer(&trailers))
	if err != nil {
		err = client.inner.handleError(handle, headers, trailers, err)
		return types.PersistentSubscriptionFailedCreationError(err)
	}

	return nil
}

func (client *persistentClient) UpdateStreamSubscription(
	ctx context.Context,
	handle connectionHandle,
	streamName string,
	groupName string,
	position stream.StreamPosition,
	settings types.SubscriptionSettings,
) error {
	updateSubscriptionConfig := protoutils.UpdatePersistentRequestStreamProto(streamName, groupName, position, settings)
	var headers, trailers metadata.MD
	_, err := client.persistentSubscriptionClient.Update(ctx, updateSubscriptionConfig, grpc.Header(&headers), grpc.Trailer(&trailers))
	if err != nil {
		err = client.inner.handleError(handle, headers, trailers, err)
		return types.PersistentSubscriptionUpdateFailedError(err)
	}

	return nil
}

func (client *persistentClient) UpdateAllSubscription(
	ctx context.Context,
	handle connectionHandle,
	groupName string,
	position stream.AllStreamPosition,
	settings types.SubscriptionSettings,
) error {
	updateSubscriptionConfig := protoutils.UpdatePersistentRequestAllOptionsProto(groupName, position, settings)

	var headers, trailers metadata.MD
	_, err := client.persistentSubscriptionClient.Update(ctx, updateSubscriptionConfig, grpc.Header(&headers), grpc.Trailer(&trailers))
	if err != nil {
		err = client.inner.handleError(handle, headers, trailers, err)
		return types.PersistentSubscriptionUpdateFailedError(err)
	}

	return nil
}

func (client *persistentClient) DeleteStreamSubscription(
	ctx context.Context,
	handle connectionHandle,
	streamName string,
	groupName string,
) error {
	deleteSubscriptionOptions := protoutils.DeletePersistentRequestStreamProto(streamName, groupName)
	var headers, trailers metadata.MD
	_, err := client.persistentSubscriptionClient.Delete(ctx, deleteSubscriptionOptions, grpc.Header(&headers), grpc.Trailer(&trailers))
	if err != nil {
		err = client.inner.handleError(handle, headers, trailers, err)
		return types.PersistentSubscriptionDeletionFailedError(err)
	}

	return nil
}

func (client *persistentClient) DeleteAllSubscription(ctx context.Context, handle connectionHandle, groupName string) error {
	deleteSubscriptionOptions := protoutils.DeletePersistentRequestAllOptionsProto(groupName)
	var headers, trailers metadata.MD
	_, err := client.persistentSubscriptionClient.Delete(ctx, deleteSubscriptionOptions, grpc.Header(&headers), grpc.Trailer(&trailers))
	if err != nil {
		err = client.inner.handleError(handle, headers, trailers, err)
		return types.PersistentSubscriptionDeletionFailedError(err)
	}

	return nil
}

func newPersistentClient(inner *grpcClient, client persistent.PersistentSubscriptionsClient) persistentClient {
	return persistentClient{
		inner:                        inner,
		persistentSubscriptionClient: client,
	}
}

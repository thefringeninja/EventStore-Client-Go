package persistent

import (
	"context"

	"github.com/EventStore/EventStore-Client-Go/stream"

	"github.com/EventStore/EventStore-Client-Go/client/filtering"
	"github.com/EventStore/EventStore-Client-Go/connection"
	persistentProto "github.com/EventStore/EventStore-Client-Go/protos/persistent"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

type Client struct {
	inner                        connection.GrpcClient
	persistentSubscriptionClient persistentProto.PersistentSubscriptionsClient
}

const (
	SubscribeToStreamSync_FailedToInitPersistentSubscriptionClientErr ErrorCode = "SubscribeToStreamSync_FailedToInitPersistentSubscriptionClientErr"
	SubscribeToStreamSync_FailedToSendStreamInitializationErr         ErrorCode = "SubscribeToStreamSync_FailedToSendStreamInitializationErr"
	SubscribeToStreamSync_FailedToReceiveStreamInitializationErr      ErrorCode = "SubscribeToStreamSync_FailedToReceiveStreamInitializationErr"
	SubscribeToStreamSync_NoSubscriptionConfirmationErr               ErrorCode = "SubscribeToStreamSync_NoSubscriptionConfirmationErr"
)

func (client Client) ConnectToPersistentSubscription(
	ctx context.Context,
	handle connection.ConnectionHandle,
	bufferSize int32,
	streamName string,
	groupName string,
) (*PersistentSubscription, error) {
	var headers, trailers metadata.MD
	ctx, cancel := context.WithCancel(ctx)
	readClient, err := client.persistentSubscriptionClient.Read(ctx, grpc.Header(&headers), grpc.Trailer(&trailers))
	if err != nil {
		defer cancel()
		err = client.inner.HandleError(handle, headers, trailers, err)
		return nil, NewError(SubscribeToStreamSync_FailedToInitPersistentSubscriptionClientErr, err)
	}

	err = readClient.Send(ToPersistentReadRequest(bufferSize, groupName, []byte(streamName)))
	if err != nil {
		defer cancel()
		return nil, NewError(SubscribeToStreamSync_FailedToSendStreamInitializationErr, err)
	}

	readResult, err := readClient.Recv()
	if err != nil {
		defer cancel()
		return nil, NewError(SubscribeToStreamSync_FailedToReceiveStreamInitializationErr, err)
	}
	switch readResult.Content.(type) {
	case *persistentProto.ReadResp_SubscriptionConfirmation_:
		{
			asyncConnection := NewPersistentSubscription(
				readClient,
				readResult.GetSubscriptionConfirmation().SubscriptionId,
				cancel)

			return asyncConnection, nil
		}
	}

	defer cancel()
	return nil, NewError(SubscribeToStreamSync_NoSubscriptionConfirmationErr, err)
}

const CreateStreamSubscription_FailedToCreatePermanentSubscriptionErr ErrorCode = "CreateStreamSubscription_FailedToCreatePermanentSubscriptionErr"

func (client Client) CreateStreamSubscription(
	ctx context.Context,
	handle connection.ConnectionHandle,
	streamName string,
	groupName string,
	position stream.StreamPosition,
	settings SubscriptionSettings,
) error {
	createSubscriptionConfig := CreatePersistentRequestProto(streamName, groupName, position, settings)
	var headers, trailers metadata.MD
	_, err := client.persistentSubscriptionClient.Create(ctx, createSubscriptionConfig, grpc.Header(&headers), grpc.Trailer(&trailers))
	if err != nil {
		err = client.inner.HandleError(handle, headers, trailers, err)
		return NewError(CreateStreamSubscription_FailedToCreatePermanentSubscriptionErr, err)
	}

	return nil
}

const (
	CreateAllSubscription_FailedToCreatePermanentSubscriptionErr ErrorCode = "CreateStreamSubscription_FailedToCreatePermanentSubscriptionErr"
	CreateAllSubscription_MustProvideRegexOrPrefixErr            ErrorCode = "CreateAllSubscription_MustProvideRegexOrPrefixErr"
	CreateAllSubscription_CanSetOnlyRegexOrPrefixErr             ErrorCode = "CreateAllSubscription_CanSetOnlyRegexOrPrefixErr"
)

func (client Client) CreateAllSubscription(
	ctx context.Context,
	handle connection.ConnectionHandle,
	groupName string,
	position stream.AllStreamPosition,
	settings SubscriptionSettings,
	filter *filtering.SubscriptionFilterOptions,
) error {
	protoConfig, err := CreatePersistentRequestAllOptionsProto(groupName, position, settings, filter)
	if err != nil {
		errorCode, ok := err.(Error)

		if ok {
			if errorCode.Code() == CreateRequestFilterOptionsProto_MustProvideRegexOrPrefixErr {
				return NewErrorCode(CreateAllSubscription_MustProvideRegexOrPrefixErr)
			} else if errorCode.Code() == CreateRequestFilterOptionsProto_CanSetOnlyRegexOrPrefixErr {
				return NewErrorCode(CreateAllSubscription_CanSetOnlyRegexOrPrefixErr)
			}
		}
		return err
	}

	var headers, trailers metadata.MD
	_, err = client.persistentSubscriptionClient.Create(ctx, protoConfig, grpc.Header(&headers), grpc.Trailer(&trailers))
	if err != nil {
		err = client.inner.HandleError(handle, headers, trailers, err)
		return NewError(CreateAllSubscription_FailedToCreatePermanentSubscriptionErr, err)
	}

	return nil
}

const UpdateStreamSubscription_FailedToUpdateErr ErrorCode = "UpdateStreamSubscription_FailedToUpdateErr"

func (client Client) UpdateStreamSubscription(
	ctx context.Context,
	handle connection.ConnectionHandle,
	streamName string,
	groupName string,
	position stream.StreamPosition,
	settings SubscriptionSettings,
) error {
	updateSubscriptionConfig := UpdatePersistentRequestStreamProto(streamName, groupName, position, settings)
	var headers, trailers metadata.MD
	_, err := client.persistentSubscriptionClient.Update(ctx, updateSubscriptionConfig, grpc.Header(&headers), grpc.Trailer(&trailers))
	if err != nil {
		err = client.inner.HandleError(handle, headers, trailers, err)
		return NewError(UpdateStreamSubscription_FailedToUpdateErr, err)
	}

	return nil
}

const UpdateAllSubscription_FailedToUpdateErr ErrorCode = "UpdateAllSubscription_FailedToUpdateErr"

func (client Client) UpdateAllSubscription(
	ctx context.Context,
	handle connection.ConnectionHandle,
	groupName string,
	position stream.AllStreamPosition,
	settings SubscriptionSettings,
) error {
	updateSubscriptionConfig := UpdatePersistentRequestAllOptionsProto(groupName, position, settings)

	var headers, trailers metadata.MD
	_, err := client.persistentSubscriptionClient.Update(ctx, updateSubscriptionConfig, grpc.Header(&headers), grpc.Trailer(&trailers))
	if err != nil {
		err = client.inner.HandleError(handle, headers, trailers, err)
		return NewError(UpdateAllSubscription_FailedToUpdateErr, err)
	}

	return nil
}

const DeleteStreamSubscription_FailedToDeleteErr ErrorCode = "DeleteStreamSubscription_FailedToDeleteErr"

func (client Client) DeleteStreamSubscription(
	ctx context.Context,
	handle connection.ConnectionHandle,
	streamName string,
	groupName string,
) error {
	deleteSubscriptionOptions := DeletePersistentRequestStreamProto(streamName, groupName)
	var headers, trailers metadata.MD
	_, err := client.persistentSubscriptionClient.Delete(ctx, deleteSubscriptionOptions, grpc.Header(&headers), grpc.Trailer(&trailers))
	if err != nil {
		err = client.inner.HandleError(handle, headers, trailers, err)
		return NewError(DeleteStreamSubscription_FailedToDeleteErr, err)
	}

	return nil
}

const DeleteAllSubscription_FailedToDeleteErr ErrorCode = "DeleteAllSubscription_FailedToDeleteErr"

func (client Client) DeleteAllSubscription(ctx context.Context, handle connection.ConnectionHandle, groupName string) error {
	deleteSubscriptionOptions := DeletePersistentRequestAllOptionsProto(groupName)
	var headers, trailers metadata.MD
	_, err := client.persistentSubscriptionClient.Delete(ctx, deleteSubscriptionOptions, grpc.Header(&headers), grpc.Trailer(&trailers))
	if err != nil {
		err = client.inner.HandleError(handle, headers, trailers, err)
		return NewError(DeleteAllSubscription_FailedToDeleteErr, err)
	}

	return nil
}

func NewClient(inner connection.GrpcClient, client persistentProto.PersistentSubscriptionsClient) Client {
	return Client{
		inner:                        inner,
		persistentSubscriptionClient: client,
	}
}

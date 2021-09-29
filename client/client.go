package client

import (
	"context"
	"fmt"
	"encoding/json"

	"github.com/EventStore/EventStore-Client-Go/connection"
	"github.com/EventStore/EventStore-Client-Go/persistent"
	persistentProto "github.com/EventStore/EventStore-Client-Go/protos/persistent"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	"github.com/EventStore/EventStore-Client-Go/errors"
	"github.com/EventStore/EventStore-Client-Go/internal/protoutils"
	"github.com/EventStore/EventStore-Client-Go/messages"
	"github.com/EventStore/EventStore-Client-Go/options"
	esdb_metadata "github.com/EventStore/EventStore-Client-Go/metadata"
	api "github.com/EventStore/EventStore-Client-Go/protos/streams"
)

type Configuration = connection.Configuration

func ParseConnectionString(str string) (*connection.Configuration, error) {
	return connection.ParseConnectionString(str)
}

// Client ...
type Client struct {
	grpcClient connection.GrpcClient
	Config     *connection.Configuration
}

// NewClient ...
func NewClient(configuration *connection.Configuration) (*Client, error) {
	grpcClient := connection.NewGrpcClient(*configuration)
	return &Client{
		grpcClient: grpcClient,
		Config:     configuration,
	}, nil
}

// Close ...
func (client *Client) Close() error {
	client.grpcClient.Close()
	return nil
}

// AppendToStream ...
func (client *Client) AppendToStream(
	context context.Context,
	streamID string,
	opts options.AppendToStreamOptions,
	events ...messages.ProposedEvent,
) (*WriteResult, error) {
	handle, err := client.grpcClient.GetConnectionHandle()
	if err != nil {
		return nil, err
	}
	streamsClient := api.NewStreamsClient(handle.Connection())
	var headers, trailers metadata.MD

	appendOperation, err := streamsClient.Append(context, grpc.Header(&headers), grpc.Trailer(&trailers))
	if err != nil {
		err = client.grpcClient.HandleError(handle, headers, trailers, err)
		return nil, fmt.Errorf("Could not construct append operation. Reason: %v", err)
	}

	header := protoutils.ToAppendHeader(streamID, opts.ExpectedRevision())

	if err := appendOperation.Send(header); err != nil {
		err = client.grpcClient.HandleError(handle, headers, trailers, err)
		return nil, fmt.Errorf("Could not send append request header. Reason: %v", err)
	}

	for _, event := range events {
		appendRequest := &api.AppendReq{
			Content: &api.AppendReq_ProposedMessage_{
				ProposedMessage: protoutils.ToProposedMessage(event),
			},
		}

		if err = appendOperation.Send(appendRequest); err != nil {
			err = client.grpcClient.HandleError(handle, headers, trailers, err)
			return nil, fmt.Errorf("Could not send append request. Reason: %v", err)
		}
	}

	response, err := appendOperation.CloseAndRecv()
	if err != nil {
		return nil, client.grpcClient.HandleError(handle, headers, trailers, err)
	}

	result := response.GetResult()
	switch result.(type) {
	case *api.AppendResp_Success_:
		{
			success := result.(*api.AppendResp_Success_)
			var streamRevision uint64
			if _, ok := success.Success.GetCurrentRevisionOption().(*api.AppendResp_Success_NoStream); ok {
				streamRevision = 1
			} else {
				streamRevision = success.Success.GetCurrentRevision()
			}

			var commitPosition uint64
			var preparePosition uint64
			if position, ok := success.Success.GetPositionOption().(*api.AppendResp_Success_Position); ok {
				commitPosition = position.Position.CommitPosition
				preparePosition = position.Position.PreparePosition
			} else {
				streamRevision = success.Success.GetCurrentRevision()
			}

			return &WriteResult{
				CommitPosition:      commitPosition,
				PreparePosition:     preparePosition,
				NextExpectedVersion: streamRevision,
			}, nil
		}
	case *api.AppendResp_WrongExpectedVersion_:
		{
			return nil, errors.ErrWrongExpectedStreamRevision
		}
	}

	return &WriteResult{
		CommitPosition:      0,
		PreparePosition:     0,
		NextExpectedVersion: 1,
	}, nil
}

func (client *Client) SetStreamMetadata(
	context context.Context,
	streamID string,
	opts options.AppendToStreamOptions,
	metadata esdb_metadata.StreamMetadata,
) (*WriteResult, error) {
	streamName := fmt.Sprintf("$$%v", streamID)
	props, err := metadata.ToMap()

	if err != nil {
		return nil, err
	}

	event := messages.ProposedEvent{}
	event.SetEventType("$metadata")
	err = event.SetJsonData(props)

	if err != nil {
		return nil, err
	}

	result, err := client.AppendToStream(context, streamName, opts, event)

	if err != nil {
		return nil, err
	}

	return result, nil
}

func (client *Client) GetStreamMetadata(
	context context.Context,
	streamID string,
	opts options.ReadStreamEventsOptions,
) (*esdb_metadata.StreamMetadata, error) {
	streamName := fmt.Sprintf("$$%v", streamID)

	stream, err := client.ReadStreamEvents(context, streamName, opts, 1)

	if err != nil {
		return nil, err
	}

	event, err := stream.Recv()

	if err == errors.ErrStreamNotFound {
		meta := esdb_metadata.StreamMetadataDefault()
		return &meta, nil
	}

	if err != nil {
		return nil, err
	}

	var props map[string]interface{}

	err = json.Unmarshal(event.GetOriginalEvent().Data, &props)

	if err != nil {
		return nil, err
	}

	meta, err := esdb_metadata.StreamMetadataFromMap(props)

	if err != nil {
		return nil, err
	}

	return &meta, nil
}

// DeleteStream ...
func (client *Client) DeleteStream(
	context context.Context,
	streamID string,
	opts options.DeleteStreamOptions,
) (*DeleteResult, error) {
	handle, err := client.grpcClient.GetConnectionHandle()
	if err != nil {
		return nil, err
	}
	streamsClient := api.NewStreamsClient(handle.Connection())
	var headers, trailers metadata.MD
	deleteRequest := protoutils.ToDeleteRequest(streamID, opts.ExpectedRevision())
	deleteResponse, err := streamsClient.Delete(context, deleteRequest, grpc.Header(&headers), grpc.Trailer(&trailers))
	if err != nil {
		err = client.grpcClient.HandleError(handle, headers, trailers, err)
		return nil, fmt.Errorf("Failed to perform delete, details: %v", err)
	}

	return &DeleteResult{Position: protoutils.DeletePositionFromProto(deleteResponse)}, nil
}

// Tombstone ...
func (client *Client) TombstoneStream(
	context context.Context,
	streamID string,
	opts options.TombstoneStreamOptions,
) (*DeleteResult, error) {
	handle, err := client.grpcClient.GetConnectionHandle()
	if err != nil {
		return nil, err
	}
	streamsClient := api.NewStreamsClient(handle.Connection())
	var headers, trailers metadata.MD
	tombstoneRequest := protoutils.ToTombstoneRequest(streamID, opts.ExpectedRevision())
	tombstoneResponse, err := streamsClient.Tombstone(context, tombstoneRequest, grpc.Header(&headers), grpc.Trailer(&trailers))

	if err != nil {
		err = client.grpcClient.HandleError(handle, headers, trailers, err)
		return nil, fmt.Errorf("Failed to perform delete, details: %v", err)
	}

	return &DeleteResult{Position: protoutils.TombstonePositionFromProto(tombstoneResponse)}, nil
}

// ReadStreamEvents ...
func (client *Client) ReadStreamEvents(
	context context.Context,
	streamID string,
	opts options.ReadStreamEventsOptions,
	count uint64,
) (*ReadStream, error) {
	readRequest := protoutils.ToReadStreamRequest(streamID, opts.Direction(), opts.Position(), count, opts.ResolveLinks())
	handle, err := client.grpcClient.GetConnectionHandle()
	if err != nil {
		return nil, err
	}
	streamsClient := api.NewStreamsClient(handle.Connection())

	return readInternal(context, client.grpcClient, handle, streamsClient, readRequest)
}

// ReadAllEvents ...
func (client *Client) ReadAllEvents(
	context context.Context,
	opts options.ReadAllEventsOptions,
	count uint64,
) (*ReadStream, error) {
	handle, err := client.grpcClient.GetConnectionHandle()
	if err != nil {
		return nil, err
	}
	streamsClient := api.NewStreamsClient(handle.Connection())
	readRequest := protoutils.ToReadAllRequest(opts.Direction(), opts.Position(), count, opts.ResolveLinks())
	return readInternal(context, client.grpcClient, handle, streamsClient, readRequest)
}

// SubscribeToStream ...
func (client *Client) SubscribeToStream(
	ctx context.Context,
	streamID string,
	opts options.SubscribeToStreamOptions,
) (*Subscription, error) {
	handle, err := client.grpcClient.GetConnectionHandle()
	if err != nil {
		return nil, err
	}
	var headers, trailers metadata.MD
	streamsClient := api.NewStreamsClient(handle.Connection())
	subscriptionRequest, err := protoutils.ToStreamSubscriptionRequest(streamID, opts.Position(), opts.ResolveLinks(), nil)
	if err != nil {
		return nil, fmt.Errorf("Failed to construct subscription. Reason: %v", err)
	}
	ctx, cancel := context.WithCancel(ctx)
	readClient, err := streamsClient.Read(ctx, subscriptionRequest, grpc.Header(&headers), grpc.Trailer(&trailers))
	if err != nil {
		defer cancel()
		err = client.grpcClient.HandleError(handle, headers, trailers, err)
		return nil, fmt.Errorf("Failed to construct subscription. Reason: %v", err)
	}
	readResult, err := readClient.Recv()
	if err != nil {
		defer cancel()
		err = client.grpcClient.HandleError(handle, headers, trailers, err)
		return nil, fmt.Errorf("Failed to perform read. Reason: %v", err)
	}
	switch readResult.Content.(type) {
	case *api.ReadResp_Confirmation:
		{
			confirmation := readResult.GetConfirmation()
			return NewSubscription(client, cancel, readClient, confirmation.SubscriptionId), nil
		}
	case *api.ReadResp_StreamNotFound_:
		{
			defer cancel()
			return nil, fmt.Errorf("Failed to initiate subscription because the stream (%s) was not found.", streamID)
		}
	}
	defer cancel()
	return nil, fmt.Errorf("Failed to initiate subscription.")
}

// SubscribeToAll ...
func (client *Client) SubscribeToAll(
	ctx context.Context,
	opts options.SubscribeToAllOptions,
) (*Subscription, error) {
	handle, err := client.grpcClient.GetConnectionHandle()
	if err != nil {
		return nil, err
	}
	streamsClient := api.NewStreamsClient(handle.Connection())
	var headers, trailers metadata.MD
	subscriptionRequest, err := protoutils.ToAllSubscriptionRequest(opts.Position(), opts.ResolveLinks(), opts.Filter())
	if err != nil {
		return nil, fmt.Errorf("failed to construct subscription. Reason: %v", err)
	}
	ctx, cancel := context.WithCancel(ctx)
	readClient, err := streamsClient.Read(ctx, subscriptionRequest, grpc.Header(&headers), grpc.Trailer(&trailers))
	if err != nil {
		defer cancel()
		err = client.grpcClient.HandleError(handle, headers, trailers, err)
		return nil, fmt.Errorf("failed to construct subscription. Reason: %v", err)
	}
	readResult, err := readClient.Recv()
	if err != nil {
		defer cancel()
		err = client.grpcClient.HandleError(handle, headers, trailers, err)
		return nil, fmt.Errorf("failed to perform read. Reason: %v", err)
	}
	switch readResult.Content.(type) {
	case *api.ReadResp_Confirmation:
		{
			confirmation := readResult.GetConfirmation()
			return NewSubscription(client, cancel, readClient, confirmation.SubscriptionId), nil
		}
	}
	defer cancel()
	return nil, fmt.Errorf("failed to initiate subscription")
}

// ConnectToPersistentSubscription ...
func (client *Client) ConnectToPersistentSubscription(
	ctx context.Context,
	streamName string,
	groupName string,
	options *options.ConnectToPersistentSubscriptionOptions,
) (persistent.SyncReadConnection, error) {
	handle, err := client.grpcClient.GetConnectionHandle()
	if err != nil {
		return nil, err
	}
	persistentSubscriptionClient := persistent.NewClient(client.grpcClient, persistentProto.NewPersistentSubscriptionsClient(handle.Connection()))

	return persistentSubscriptionClient.SubscribeToStreamSync(
		ctx,
		handle,
		options.GetBatchSize(),
		streamName,
		groupName,
	)
}

func (client *Client) CreatePersistentSubscription(
	ctx context.Context,
	streamName string,
	groupName string,
	options *options.PersistentStreamSubscriptionOptions,
) error {
	handle, err := client.grpcClient.GetConnectionHandle()
	if err != nil {
		return err
	}
	persistentSubscriptionClient := persistent.NewClient(client.grpcClient, persistentProto.NewPersistentSubscriptionsClient(handle.Connection()))

	return persistentSubscriptionClient.CreateStreamSubscription(ctx, handle, streamName, groupName, options.GetPosition(), options.GetSettings())
}

func (client *Client) CreatePersistentSubscriptionAll(
	ctx context.Context,
	groupName string,
	options *options.PersistentAllSubscriptionOptions,
) error {
	handle, err := client.grpcClient.GetConnectionHandle()
	if err != nil {
		return err
	}
	persistentSubscriptionClient := persistent.NewClient(client.grpcClient, persistentProto.NewPersistentSubscriptionsClient(handle.Connection()))

	return persistentSubscriptionClient.CreateAllSubscription(
		ctx,
		handle,
		groupName,
		options.GetPosition(),
		options.GetSettings(),
		options.GetFilter(),
	)
}

func (client *Client) UpdatePersistentStreamSubscription(
	ctx context.Context,
	streamName string,
	groupName string,
	options *options.PersistentStreamSubscriptionOptions,
) error {
	handle, err := client.grpcClient.GetConnectionHandle()
	if err != nil {
		return err
	}
	persistentSubscriptionClient := persistent.NewClient(client.grpcClient, persistentProto.NewPersistentSubscriptionsClient(handle.Connection()))

	return persistentSubscriptionClient.UpdateStreamSubscription(ctx, handle, streamName, groupName, options.GetPosition(), options.GetSettings())
}

func (client *Client) UpdatePersistentSubscriptionAll(
	ctx context.Context,
	groupName string,
	options *options.PersistentAllSubscriptionOptions,
) error {
	handle, err := client.grpcClient.GetConnectionHandle()
	if err != nil {
		return err
	}
	persistentSubscriptionClient := persistent.NewClient(client.grpcClient, persistentProto.NewPersistentSubscriptionsClient(handle.Connection()))

	return persistentSubscriptionClient.UpdateAllSubscription(ctx, handle, groupName, options.GetPosition(), options.GetSettings())
}

func (client *Client) DeletePersistentSubscription(
	ctx context.Context,
	streamName string,
	groupName string,
) error {
	handle, err := client.grpcClient.GetConnectionHandle()
	if err != nil {
		return err
	}
	persistentSubscriptionClient := persistent.NewClient(client.grpcClient, persistentProto.NewPersistentSubscriptionsClient(handle.Connection()))

	return persistentSubscriptionClient.DeleteStreamSubscription(ctx, handle, streamName, groupName)
}

func (client *Client) DeletePersistentSubscriptionAll(
	ctx context.Context,
	groupName string,
) error {
	handle, err := client.grpcClient.GetConnectionHandle()
	if err != nil {
		return err
	}
	persistentSubscriptionClient := persistent.NewClient(client.grpcClient, persistentProto.NewPersistentSubscriptionsClient(handle.Connection()))

	return persistentSubscriptionClient.DeleteAllSubscription(ctx, handle, groupName)
}

func readInternal(
	ctx context.Context,
	client connection.GrpcClient,
	handle connection.ConnectionHandle,
	streamsClient api.StreamsClient,
	readRequest *api.ReadReq,
) (*ReadStream, error) {
	var headers, trailers metadata.MD
	ctx, cancel := context.WithCancel(ctx)
	result, err := streamsClient.Read(ctx, readRequest, grpc.Header(&headers), grpc.Trailer(&trailers))
	if err != nil {
		defer cancel()
		err = client.HandleError(handle, headers, trailers, err)
		return nil, fmt.Errorf("failed to construct read stream. Reason: %v", err)
	}

	params := ReadStreamParams{
		client:   client,
		handle:   handle,
		cancel:   cancel,
		inner:    result,
		headers:  headers,
		trailers: trailers,
	}

	stream := NewReadStream(params)

	return stream, nil
}

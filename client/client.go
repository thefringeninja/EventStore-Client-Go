package client

import (
	"context"
	"encoding/json"
	"fmt"
	"io"

	"errors"

	"github.com/EventStore/EventStore-Client-Go/internal/protoutils"
	"github.com/EventStore/EventStore-Client-Go/types"

	persistentProto "github.com/EventStore/EventStore-Client-Go/protos/persistent"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	api "github.com/EventStore/EventStore-Client-Go/protos/streams"
)

// Client ...
type Client struct {
	grpcClient *grpcClient
	Config     *Configuration
}

// NewClient ...
func NewClient(configuration *Configuration) (*Client, error) {
	grpcClient := NewGrpcClient(*configuration)
	return &Client{
		grpcClient: grpcClient,
		Config:     configuration,
	}, nil
}

// Close ...
func (client *Client) Close() error {
	client.grpcClient.close()
	return nil
}

// AppendToStream ...
func (client *Client) AppendToStream(
	context context.Context,
	streamID string,
	opts AppendToStreamOptions,
	events ...types.ProposedEvent,
) (*WriteResult, error) {
	opts.setDefaults()
	handle, err := client.grpcClient.getConnectionHandle()
	if err != nil {
		return nil, fmt.Errorf("can't get a connection handle: %w", err)
	}
	streamsClient := api.NewStreamsClient(handle.Connection())
	var headers, trailers metadata.MD

	appendOperation, err := streamsClient.Append(context, grpc.Header(&headers), grpc.Trailer(&trailers))
	if err != nil {
		err = client.grpcClient.handleError(handle, headers, trailers, err)
		return nil, fmt.Errorf("could not construct append operation. Reason: %w", err)
	}

	header := protoutils.ToAppendHeader(streamID, opts.ExpectedRevision)

	if err := appendOperation.Send(header); err != nil {
		err = client.grpcClient.handleError(handle, headers, trailers, err)
		return nil, fmt.Errorf("could not send append request header. Reason: %w", err)
	}

	for _, event := range events {
		appendRequest := &api.AppendReq{
			Content: &api.AppendReq_ProposedMessage_{
				ProposedMessage: protoutils.ToProposedMessage(event),
			},
		}

		if err = appendOperation.Send(appendRequest); err != nil {
			err = client.grpcClient.handleError(handle, headers, trailers, err)
			return nil, fmt.Errorf("could not send append request. Reason: %w", err)
		}
	}

	response, err := appendOperation.CloseAndRecv()
	if err != nil {
		return nil, client.grpcClient.handleError(handle, headers, trailers, err)
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
			return nil, ErrWrongExpectedStreamRevision
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
	opts AppendToStreamOptions,
	metadata types.StreamMetadata,
) (*WriteResult, error) {
	streamName := fmt.Sprintf("$$%v", streamID)
	props, err := metadata.ToMap()

	if err != nil {
		return nil, fmt.Errorf("error when serializing stream metadata: %w", err)
	}

	event := types.ProposedEvent{}
	event.SetEventType("$metadata")
	err = event.SetJsonData(props)

	if err != nil {
		return nil, fmt.Errorf("error when serializing stream metadata: %w", err)
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
	opts ReadStreamEventsOptions,
) (*types.StreamMetadata, error) {
	streamName := fmt.Sprintf("$$%v", streamID)

	stream, err := client.ReadStreamEvents(context, streamName, opts, 1)

	var streamDeletedError *StreamDeletedError
	if errors.Is(err, ErrStreamNotFound) || errors.As(err, &streamDeletedError) {
		return nil, err
	}

	if err != nil {
		return nil, fmt.Errorf("unexpected error when reading stream metadata: %w", err)
	}

	event, err := stream.Recv()

	if errors.Is(err, io.EOF) {
		return &types.StreamMetadata{}, nil
	}

	if err != nil {
		return nil, fmt.Errorf("unexpected error when reading stream metadata: %w", err)
	}

	var props map[string]interface{}

	err = json.Unmarshal(event.OriginalEvent().Data, &props)

	if err != nil {
		return nil, fmt.Errorf("error when deserializing stream metadata json: %w", err)
	}

	meta, err := types.StreamMetadataFromMap(props)

	if err != nil {
		return nil, fmt.Errorf("error when parsing stream metadata json: %w", err)
	}

	return &meta, nil
}

// DeleteStream ...
func (client *Client) DeleteStream(
	context context.Context,
	streamID string,
	opts DeleteStreamOptions,
) (*DeleteResult, error) {
	opts.setDefaults()
	handle, err := client.grpcClient.getConnectionHandle()
	if err != nil {
		return nil, fmt.Errorf("can't get a connection handle: %w", err)
	}
	streamsClient := api.NewStreamsClient(handle.Connection())
	var headers, trailers metadata.MD
	deleteRequest := protoutils.ToDeleteRequest(streamID, opts.ExpectedRevision)
	deleteResponse, err := streamsClient.Delete(context, deleteRequest, grpc.Header(&headers), grpc.Trailer(&trailers))
	if err != nil {
		err = client.grpcClient.handleError(handle, headers, trailers, err)
		return nil, fmt.Errorf("failed to perform delete, details: %w", err)
	}

	return &DeleteResult{Position: protoutils.DeletePositionFromProto(deleteResponse)}, nil
}

// Tombstone ...
func (client *Client) TombstoneStream(
	context context.Context,
	streamID string,
	opts TombstoneStreamOptions,
) (*DeleteResult, error) {
	opts.setDefaults()
	handle, err := client.grpcClient.getConnectionHandle()
	if err != nil {
		return nil, fmt.Errorf("can't get a connection handle: %w", err)
	}
	streamsClient := api.NewStreamsClient(handle.Connection())
	var headers, trailers metadata.MD
	tombstoneRequest := protoutils.ToTombstoneRequest(streamID, opts.ExpectedRevision)
	tombstoneResponse, err := streamsClient.Tombstone(context, tombstoneRequest, grpc.Header(&headers), grpc.Trailer(&trailers))

	if err != nil {
		err = client.grpcClient.handleError(handle, headers, trailers, err)
		return nil, fmt.Errorf("failed to perform delete, details: %w", err)
	}

	return &DeleteResult{Position: protoutils.TombstonePositionFromProto(tombstoneResponse)}, nil
}

// ReadStreamEvents ...
func (client *Client) ReadStreamEvents(
	context context.Context,
	streamID string,
	opts ReadStreamEventsOptions,
	count uint64,
) (*ReadStream, error) {
	opts.setDefaults()
	readRequest := protoutils.ToReadStreamRequest(streamID, opts.Direction, opts.From, count, opts.ResolveLinks)
	handle, err := client.grpcClient.getConnectionHandle()
	if err != nil {
		return nil, fmt.Errorf("can't get a connection handle: %w", err)
	}
	streamsClient := api.NewStreamsClient(handle.Connection())

	return readInternal(context, client.grpcClient, handle, streamsClient, readRequest)
}

// ReadAllEvents ...
func (client *Client) ReadAllEvents(
	context context.Context,
	opts ReadAllEventsOptions,
	count uint64,
) (*ReadStream, error) {
	opts.setDefaults()
	handle, err := client.grpcClient.getConnectionHandle()
	if err != nil {
		return nil, fmt.Errorf("can't get a connection handle: %w", err)
	}
	streamsClient := api.NewStreamsClient(handle.Connection())
	readRequest := protoutils.ToReadAllRequest(opts.Direction, opts.From, count, opts.ResolveLinks)
	return readInternal(context, client.grpcClient, handle, streamsClient, readRequest)
}

// SubscribeToStream ...
func (client *Client) SubscribeToStream(
	ctx context.Context,
	streamID string,
	opts SubscribeToStreamOptions,
) (*Subscription, error) {
	opts.setDefaults()
	handle, err := client.grpcClient.getConnectionHandle()
	if err != nil {
		return nil, fmt.Errorf("can't get a connection handle: %w", err)
	}
	var headers, trailers metadata.MD
	streamsClient := api.NewStreamsClient(handle.Connection())
	subscriptionRequest, err := protoutils.ToStreamSubscriptionRequest(streamID, opts.From, opts.ResolveLinks, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to construct subscription. Reason: %w", err)
	}
	ctx, cancel := context.WithCancel(ctx)
	readClient, err := streamsClient.Read(ctx, subscriptionRequest, grpc.Header(&headers), grpc.Trailer(&trailers))
	if err != nil {
		defer cancel()
		err = client.grpcClient.handleError(handle, headers, trailers, err)
		return nil, fmt.Errorf("failed to construct subscription. Reason: %w", err)
	}
	readResult, err := readClient.Recv()
	if err != nil {
		defer cancel()
		err = client.grpcClient.handleError(handle, headers, trailers, err)
		return nil, fmt.Errorf("failed to perform read. Reason: %w", err)
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

// SubscribeToAll ...
func (client *Client) SubscribeToAll(
	ctx context.Context,
	opts SubscribeToAllOptions,
) (*Subscription, error) {
	opts.setDefaults()
	handle, err := client.grpcClient.getConnectionHandle()
	if err != nil {
		return nil, fmt.Errorf("can't get a connection handle: %w", err)
	}
	streamsClient := api.NewStreamsClient(handle.Connection())
	var headers, trailers metadata.MD

	var filterOptions *protoutils.SubscriptionFilterOptions = nil
	if opts.Filter != nil {
		filterOptions = &protoutils.SubscriptionFilterOptions{
			MaxSearchWindow:    opts.MaxSearchWindow,
			CheckpointInterval: opts.CheckpointInterval,
			SubscriptionFilter: opts.Filter,
		}
	}

	subscriptionRequest, err := protoutils.ToAllSubscriptionRequest(opts.From, opts.ResolveLinks, filterOptions)
	if err != nil {
		return nil, fmt.Errorf("failed to construct subscription. Reason: %w", err)
	}
	ctx, cancel := context.WithCancel(ctx)
	readClient, err := streamsClient.Read(ctx, subscriptionRequest, grpc.Header(&headers), grpc.Trailer(&trailers))
	if err != nil {
		defer cancel()
		err = client.grpcClient.handleError(handle, headers, trailers, err)
		return nil, fmt.Errorf("failed to construct subscription. Reason: %w", err)
	}
	readResult, err := readClient.Recv()
	if err != nil {
		defer cancel()
		err = client.grpcClient.handleError(handle, headers, trailers, err)
		return nil, fmt.Errorf("failed to perform read. Reason: %w", err)
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
	options ConnectToPersistentSubscriptionOptions,
) (*PersistentSubscription, error) {
	options.setDefaults()
	handle, err := client.grpcClient.getConnectionHandle()
	if err != nil {
		return nil, fmt.Errorf("can't get a connection handle: %w", err)
	}
	persistentSubscriptionClient := newPersistentClient(client.grpcClient, persistentProto.NewPersistentSubscriptionsClient(handle.Connection()))

	return persistentSubscriptionClient.ConnectToPersistentSubscription(
		ctx,
		handle,
		int32(options.BatchSize),
		streamName,
		groupName,
	)
}

func (client *Client) CreatePersistentSubscription(
	ctx context.Context,
	streamName string,
	groupName string,
	options PersistentStreamSubscriptionOptions,
) error {
	options.setDefaults()
	handle, err := client.grpcClient.getConnectionHandle()
	if err != nil {
		return fmt.Errorf("can't get a connection handle: %w", err)
	}
	persistentSubscriptionClient := newPersistentClient(client.grpcClient, persistentProto.NewPersistentSubscriptionsClient(handle.Connection()))

	if options.Settings == nil {
		setts := types.SubscriptionSettingsDefault()
		options.Settings = &setts
	}

	return persistentSubscriptionClient.CreateStreamSubscription(ctx, handle, streamName, groupName, options.From, *options.Settings)
}

func (client *Client) CreatePersistentSubscriptionAll(
	ctx context.Context,
	groupName string,
	options PersistentAllSubscriptionOptions,
) error {
	options.setDefaults()
	handle, err := client.grpcClient.getConnectionHandle()
	if err != nil {
		return fmt.Errorf("can't get a connection handle: %w", err)
	}

	var filterOptions *protoutils.SubscriptionFilterOptions = nil
	if options.Filter != nil {
		filterOptions = &protoutils.SubscriptionFilterOptions{
			MaxSearchWindow:    options.MaxSearchWindow,
			CheckpointInterval: options.CheckpointInterval,
			SubscriptionFilter: options.Filter,
		}
	}
	persistentSubscriptionClient := newPersistentClient(client.grpcClient, persistentProto.NewPersistentSubscriptionsClient(handle.Connection()))

	if options.Settings == nil {
		setts := types.SubscriptionSettingsDefault()
		options.Settings = &setts
	}

	return persistentSubscriptionClient.CreateAllSubscription(
		ctx,
		handle,
		groupName,
		options.From,
		*options.Settings,
		filterOptions,
	)
}

func (client *Client) UpdatePersistentStreamSubscription(
	ctx context.Context,
	streamName string,
	groupName string,
	options PersistentStreamSubscriptionOptions,
) error {
	options.setDefaults()
	handle, err := client.grpcClient.getConnectionHandle()
	if err != nil {
		return fmt.Errorf("can't get a connection handle: %w", err)
	}
	persistentSubscriptionClient := newPersistentClient(client.grpcClient, persistentProto.NewPersistentSubscriptionsClient(handle.Connection()))

	if options.Settings == nil {
		setts := types.SubscriptionSettingsDefault()
		options.Settings = &setts
	}

	return persistentSubscriptionClient.UpdateStreamSubscription(ctx, handle, streamName, groupName, options.From, *options.Settings)
}

func (client *Client) UpdatePersistentSubscriptionAll(
	ctx context.Context,
	groupName string,
	options PersistentAllSubscriptionOptions,
) error {
	options.setDefaults()
	handle, err := client.grpcClient.getConnectionHandle()
	if err != nil {
		return fmt.Errorf("can't get a connection handle: %w", err)
	}
	persistentSubscriptionClient := newPersistentClient(client.grpcClient, persistentProto.NewPersistentSubscriptionsClient(handle.Connection()))

	return persistentSubscriptionClient.UpdateAllSubscription(ctx, handle, groupName, options.From, *options.Settings)
}

func (client *Client) DeletePersistentSubscription(
	ctx context.Context,
	streamName string,
	groupName string,
) error {
	handle, err := client.grpcClient.getConnectionHandle()
	if err != nil {
		return fmt.Errorf("can't get a connection handle: %w", err)
	}
	persistentSubscriptionClient := newPersistentClient(client.grpcClient, persistentProto.NewPersistentSubscriptionsClient(handle.Connection()))

	return persistentSubscriptionClient.DeleteStreamSubscription(ctx, handle, streamName, groupName)
}

func (client *Client) DeletePersistentSubscriptionAll(
	ctx context.Context,
	groupName string,
) error {
	handle, err := client.grpcClient.getConnectionHandle()
	if err != nil {
		return fmt.Errorf("can't get a connection handle: %w", err)
	}
	persistentSubscriptionClient := newPersistentClient(client.grpcClient, persistentProto.NewPersistentSubscriptionsClient(handle.Connection()))

	return persistentSubscriptionClient.DeleteAllSubscription(ctx, handle, groupName)
}

func readInternal(
	ctx context.Context,
	client *grpcClient,
	handle connectionHandle,
	streamsClient api.StreamsClient,
	readRequest *api.ReadReq,
) (*ReadStream, error) {
	var headers, trailers metadata.MD
	ctx, cancel := context.WithCancel(ctx)
	result, err := streamsClient.Read(ctx, readRequest, grpc.Header(&headers), grpc.Trailer(&trailers))
	if err != nil {
		defer cancel()

		err = client.handleError(handle, headers, trailers, err)
		return nil, fmt.Errorf("failed to construct read stream. Reason: %w", err)
	}

	msg, err := result.Recv()
	if err != nil {
		defer cancel()
		values := trailers.Get("exception")

		if values != nil && values[0] == "stream-deleted" {
			values = trailers.Get("stream-name")
			streamName := ""

			if values != nil {
				streamName = values[0]
			}

			return nil, &StreamDeletedError{StreamName: streamName}
		}

		return nil, err
	}

	switch msg.Content.(type) {
	case *api.ReadResp_Event:
		resolvedEvent := protoutils.GetResolvedEventFromProto(msg.GetEvent())
		params := readStreamParams{
			client:   client,
			handle:   handle,
			cancel:   cancel,
			inner:    result,
			headers:  headers,
			trailers: trailers,
		}

		stream := newReadStream(params, resolvedEvent)
		return stream, nil
	case *api.ReadResp_StreamNotFound_:
		defer cancel()
		return nil, ErrStreamNotFound
	}

	defer cancel()
	return nil, fmt.Errorf("unexpected code path in readInternal")
}

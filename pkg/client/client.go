package client

import (
	"context"
	"crypto/tls"
	"encoding/base64"
	"fmt"
	"io"
	"log"

	direction "github.com/eventstore/EventStore-Client-Go/pkg/direction"
	errors "github.com/eventstore/EventStore-Client-Go/pkg/errors"
	messages "github.com/eventstore/EventStore-Client-Go/pkg/messages"
	position "github.com/eventstore/EventStore-Client-Go/pkg/position"
	protoutils "github.com/eventstore/EventStore-Client-Go/pkg/protoutils"
	stream_revision "github.com/eventstore/EventStore-Client-Go/pkg/streamrevision"
	system_metadata "github.com/eventstore/EventStore-Client-Go/pkg/systemmetadata"
	api "github.com/eventstore/EventStore-Client-Go/protos"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"
)

type basicAuth struct {
	username string
	password string
}

func (b basicAuth) GetRequestMetadata(tx context.Context, in ...string) (map[string]string, error) {
	auth := b.username + ":" + b.password
	enc := base64.StdEncoding.EncodeToString([]byte(auth))
	return map[string]string{
		"Authorization": "Basic " + enc,
	}, nil
}

func (basicAuth) RequireTransportSecurity() bool {
	return false
}

// Configuration ...
type Configuration struct {
	Address                     string
	Username                    string
	Password                    string
	SkipCertificateVerification bool
	Connected                   bool
}

// NewConfiguration ...
func NewConfiguration() *Configuration {
	return &Configuration{
		Address:                     "localhost:2113",
		Username:                    "admin",
		Password:                    "changeit",
		SkipCertificateVerification: false,
	}
}

// Client ...
type Client struct {
	Config     *Configuration
	Connection *grpc.ClientConn
}

// NewClient ...
func NewClient(configuration *Configuration) (*Client, error) {
	return &Client{
		Config: configuration,
	}, nil
}

// Connect ...
func (client *Client) Connect() error {
	config := &tls.Config{
		InsecureSkipVerify: client.Config.SkipCertificateVerification,
	}
	conn, err := grpc.Dial(client.Config.Address, grpc.WithTransportCredentials(credentials.NewTLS(config)), grpc.WithPerRPCCredentials(basicAuth{
		username: client.Config.Username,
		password: client.Config.Password,
	}))
	if err != nil {
		log.Printf("Failed to initialize connection to %+v. Details: %+v", client.Config, err)
		return err
	}
	client.Connection = conn
	return nil
}

// Close ...
func (client *Client) Close() error {
	return client.Connection.Close()
}

// AppendToStream ...
func (client *Client) AppendToStream(context context.Context, streamID string, streamRevision stream_revision.StreamRevision, events []messages.ProposedEvent) (*AppendResult, error) {
	streamsClient := api.NewStreamsClient(client.Connection)

	appendOperation, err := streamsClient.Append(context)
	if err != nil {
		log.Printf("Could not construct append operation: %v", err)
		return nil, err
	}

	header := protoutils.ToAppendHeaderFromStreamIDAndStreamRevision(streamID, streamRevision)

	if err := appendOperation.Send(header); err != nil {
		log.Printf("Could not send append request header %+v", err)
		return nil, err
	}

	for _, event := range events {
		appendRequest := &api.AppendReq{
			Content: &api.AppendReq_ProposedMessage_{
				ProposedMessage: protoutils.ToProposedMessage(event),
			},
		}

		if err = appendOperation.Send(appendRequest); err != nil {
			log.Printf("Coud not send append request: %v", err)
			return nil, err
		}
	}

	response, err := appendOperation.CloseAndRecv()
	if err != nil {
		status, _ := status.FromError(err)
		if status.Code() == 9 { //Precondition - ErrWrongExpectedStremRevision
			return nil, fmt.Errorf("%w", errors.ErrWrongExpectedStreamRevision)
		}
		if status.Code() == 7 { //PermissionDenied - ErrPemissionDenied
			return nil, fmt.Errorf("%w", errors.ErrPermissionDenied)
		}
		return nil, err
	}
	return AppendResultFromAppendResp(response), nil
}

// ReadStreamEvents ...
func (client *Client) ReadStreamEvents(context context.Context, direction direction.Direction, streamID string, streamRevision uint64, count int32, resolveLinks bool) ([]messages.RecordedEvent, error) {
	readReq := &api.ReadReq{
		Options: &api.ReadReq_Options{
			CountOption: &api.ReadReq_Options_Count{
				Count: count,
			},
			FilterOption: &api.ReadReq_Options_NoFilter{
				NoFilter: &api.ReadReq_Empty{},
			},
			ReadDirection: protoutils.ToReadDirectionFromDirection(direction),
			ResolveLinks:  resolveLinks,
			StreamOption:  protoutils.ToReadStreamOptionsFromStreamAndStreamRevision(streamID, streamRevision),
			UuidOption: &api.ReadReq_Options_UUIDOption{
				Content: &api.ReadReq_Options_UUIDOption_String_{
					String_: &api.ReadReq_Empty{},
				},
			},
		},
	}
	return readInternal(context, client.Connection, readReq, count)
}

// ReadAllEvents ...
func (client *Client) ReadAllEvents(context context.Context, direction direction.Direction, position position.Position, count int32, resolveLinks bool) ([]messages.RecordedEvent, error) {
	readReq := &api.ReadReq{
		Options: &api.ReadReq_Options{
			CountOption: &api.ReadReq_Options_Count{
				Count: count,
			},
			FilterOption: &api.ReadReq_Options_NoFilter{
				NoFilter: &api.ReadReq_Empty{},
			},
			ReadDirection: protoutils.ToReadDirectionFromDirection(direction),
			ResolveLinks:  resolveLinks,
			StreamOption:  protoutils.ToAllReadOptionsFromPosition(position),
			UuidOption: &api.ReadReq_Options_UUIDOption{
				Content: &api.ReadReq_Options_UUIDOption_String_{
					String_: &api.ReadReq_Empty{},
				},
			},
		},
	}
	return readInternal(context, client.Connection, readReq, count)
}

func readInternal(context context.Context, connection *grpc.ClientConn, readRequest *api.ReadReq, limit int32) ([]messages.RecordedEvent, error) {
	streamsClient := api.NewStreamsClient(connection)

	result, err := streamsClient.Read(context, readRequest)

	if err != nil {
		log.Fatalf("could not read: %v", err)
	}

	events := []messages.RecordedEvent{}
	for {
		readResult, err := result.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatalf("Failed to perform read. Reason: %+v", err)
		}
		readEvent := readResult.Event
		recordedEvent := readEvent.GetEvent()

		events = append(events, messages.RecordedEvent{
			EventID:        protoutils.EventIDFromProto(recordedEvent),
			EventType:      recordedEvent.Metadata[system_metadata.SystemMetadataKeysType],
			IsJSON:         protoutils.IsJSONFromProto(recordedEvent),
			StreamID:       recordedEvent.GetStreamName(),
			StreamRevision: recordedEvent.GetStreamRevision(),
			CreatedDate:    protoutils.CreatedFromProto(recordedEvent),
			Position:       protoutils.PositionFromProto(recordedEvent),
			Data:           recordedEvent.GetData(),
			SystemMetadata: recordedEvent.GetMetadata(),
			UserMetadata:   recordedEvent.GetCustomMetadata(),
		})
		if int32(len(events)) >= limit {
			break
		}
	}
	return events, nil
}

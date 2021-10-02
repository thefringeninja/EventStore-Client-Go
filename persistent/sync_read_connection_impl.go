package persistent

import (
	"context"
	"fmt"
	"log"
	"sync"

	"github.com/EventStore/EventStore-Client-Go/subscription"

	"github.com/EventStore/EventStore-Client-Go/messages"

	"github.com/EventStore/EventStore-Client-Go/protos/persistent"
	"github.com/EventStore/EventStore-Client-Go/protos/shared"
	"github.com/gofrs/uuid"
)

const MAX_ACK_COUNT = 2000

type syncReadConnectionImpl struct {
	client         protoClient
	subscriptionId string
	channel        chan request
	cancel         context.CancelFunc
	once           *sync.Once
}

const (
	Read_FailedToRead_Err                     ErrorCode = "Read_FailedToRead_Err"
	Read_ReceivedSubscriptionConfirmation_Err ErrorCode = "Read_ReceivedSubscriptionConfirmation_Err"
	Read_UnknownContentTypeReceived_Err       ErrorCode = "Read_UnknownContentTypeReceived_Err"
)

func (connection *syncReadConnectionImpl) Recv() *subscription.Event {
	channel := make(chan *subscription.Event)
	req := request{
		channel: channel,
	}

	connection.channel <- req
	resp := <-channel

	return resp
}

func (connection *syncReadConnectionImpl) Close() error {
	connection.once.Do(connection.cancel)
	return nil
}

var Exceeds_Max_Message_Count_Err ErrorCode = "Exceeds_Max_Message_Count_Err"

func (connection *syncReadConnectionImpl) Ack(messages ...*messages.ResolvedEvent) error {
	if len(messages) == 0 {
		return nil
	}

	if len(messages) > MAX_ACK_COUNT {
		return NewErrorCode(Exceeds_Max_Message_Count_Err)
	}

	ids := []uuid.UUID{}
	for _, event := range messages {
		ids = append(ids, event.OriginalEvent().EventID)
	}

	err := connection.client.Send(&persistent.ReadReq{
		Content: &persistent.ReadReq_Ack_{
			Ack: &persistent.ReadReq_Ack{
				Id:  []byte(connection.subscriptionId),
				Ids: messageIdSliceToProto(ids...),
			},
		},
	})
	if err != nil {
		return err
	}

	return nil
}

func (connection *syncReadConnectionImpl) Nack(reason string, action Nack_Action, messages ...*messages.ResolvedEvent) error {
	if len(messages) == 0 {
		return nil
	}

	ids := []uuid.UUID{}
	for _, event := range messages {
		ids = append(ids, event.OriginalEvent().EventID)
	}

	err := connection.client.Send(&persistent.ReadReq{
		Content: &persistent.ReadReq_Nack_{
			Nack: &persistent.ReadReq_Nack{
				Id:     []byte(connection.subscriptionId),
				Ids:    messageIdSliceToProto(ids...),
				Action: persistent.ReadReq_Nack_Action(action),
				Reason: reason,
			},
		},
	})
	if err != nil {
		return err
	}

	return nil
}

func messageIdSliceToProto(messageIds ...uuid.UUID) []*shared.UUID {
	result := make([]*shared.UUID, len(messageIds))

	for index, messageId := range messageIds {
		result[index] = ToProtoUUID(messageId)
	}

	return result
}

type request struct {
	channel chan *subscription.Event
}

func newSyncReadConnection(
	client protoClient,
	subscriptionId string,
	cancel context.CancelFunc,
) SyncReadConnection {
	channel := make(chan request)
	once := new(sync.Once)

	// It is not safe to consume a stream in different goroutines. This is why we only consume
	// the stream in a dedicated goroutine.
	//
	// Current implementation doesn't terminate the goroutine. When a subscription is dropped,
	// we keep user requests coming but will always send back a subscription dropped event.
	// This implementation is simple to maintain while letting the user sharing their subscription
	// among as many goroutines as they want.
	go func() {
		closed := false

		for {
			req := <-channel

			if closed {
				req.channel <- &subscription.Event{
					SubscriptionDropped: &subscription.Dropped{
						Error: fmt.Errorf("subscription has been dropped"),
					},
				}

				continue
			}

			result, err := client.Recv()
			if err != nil {
				log.Printf("[error] subscription has dropped. Reason: %v", err)

				dropped := subscription.Dropped{
					Error: err,
				}

				req.channel <- &subscription.Event{
					SubscriptionDropped: &dropped,
				}

				closed = true

				continue
			}

			switch result.Content.(type) {
			case *persistent.ReadResp_Event:
				{
					resolvedEvent := FromPersistentProtoResponse(result)
					req.channel <- &subscription.Event{
						EventAppeared: resolvedEvent,
					}
				}
			}
		}
	}()

	return &syncReadConnectionImpl{
		client:         client,
		subscriptionId: subscriptionId,
		channel:        channel,
		once:           once,
		cancel:         cancel,
	}
}

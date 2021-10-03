package client

import (
	"context"
	"fmt"

	"github.com/EventStore/EventStore-Client-Go/internal/protoutils"
	"github.com/EventStore/EventStore-Client-Go/types"

	"log"
	"sync"

	"github.com/EventStore/EventStore-Client-Go/protos/persistent"
	"github.com/EventStore/EventStore-Client-Go/protos/shared"
	"github.com/gofrs/uuid"
)

const MAX_ACK_COUNT = 2000

type Nack_Action int32

const (
	Nack_Unknown Nack_Action = 0
	Nack_Park    Nack_Action = 1
	Nack_Retry   Nack_Action = 2
	Nack_Skip    Nack_Action = 3
	Nack_Stop    Nack_Action = 4
)

type PersistentSubscription struct {
	client         persistent.PersistentSubscriptions_ReadClient
	subscriptionId string
	channel        chan persistentRequest
	cancel         context.CancelFunc
	once           *sync.Once
}

func (connection *PersistentSubscription) Recv() *types.SubscriptionEvent {
	channel := make(chan *types.SubscriptionEvent)
	req := persistentRequest{
		channel: channel,
	}

	connection.channel <- req
	resp := <-channel

	return resp
}

func (connection *PersistentSubscription) Close() error {
	connection.once.Do(connection.cancel)
	return nil
}

func (connection *PersistentSubscription) Ack(messages ...*types.ResolvedEvent) error {
	if len(messages) == 0 {
		return nil
	}

	if len(messages) > MAX_ACK_COUNT {
		return &types.PersistentSubscriptionExceedsMaxMessageCountError
	}

	var ids []uuid.UUID
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

func (connection *PersistentSubscription) Nack(reason string, action Nack_Action, messages ...*types.ResolvedEvent) error {
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
		result[index] = protoutils.ToProtoUUID(messageId)
	}

	return result
}

type persistentRequest struct {
	channel chan *types.SubscriptionEvent
}

func NewPersistentSubscription(
	client persistent.PersistentSubscriptions_ReadClient,
	subscriptionId string,
	cancel context.CancelFunc,
) *PersistentSubscription {
	channel := make(chan persistentRequest)
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
				req.channel <- &types.SubscriptionEvent{
					SubscriptionDropped: &types.SubscriptionDropped{
						Error: fmt.Errorf("subscription has been dropped"),
					},
				}

				continue
			}

			result, err := client.Recv()
			if err != nil {
				log.Printf("[error] subscription has dropped. Reason: %v", err)

				dropped := types.SubscriptionDropped{
					Error: err,
				}

				req.channel <- &types.SubscriptionEvent{
					SubscriptionDropped: &dropped,
				}

				closed = true

				continue
			}

			switch result.Content.(type) {
			case *persistent.ReadResp_Event:
				{
					resolvedEvent := protoutils.FromPersistentProtoResponse(result)
					req.channel <- &types.SubscriptionEvent{
						EventAppeared: resolvedEvent,
					}
				}
			}
		}
	}()

	return &PersistentSubscription{
		client:         client,
		subscriptionId: subscriptionId,
		channel:        channel,
		once:           once,
		cancel:         cancel,
	}
}

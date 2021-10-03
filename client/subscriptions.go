package client

import (
	"context"
	"fmt"
	"log"
	"sync"

	"github.com/EventStore/EventStore-Client-Go/internal/protoutils"
	api "github.com/EventStore/EventStore-Client-Go/protos/streams"
	"github.com/EventStore/EventStore-Client-Go/subscription"
	"github.com/EventStore/EventStore-Client-Go/types"
)

type request struct {
	channel chan *subscription.Event
}

type Subscription struct {
	client  *Client
	id      string
	inner   api.Streams_ReadClient
	channel chan request
	cancel  context.CancelFunc
	once    *sync.Once
}

func NewSubscription(client *Client, cancel context.CancelFunc, inner api.Streams_ReadClient, id string) *Subscription {
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

			result, err := inner.Recv()
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
			case *api.ReadResp_Checkpoint_:
				{
					checkpoint := result.GetCheckpoint()
					position := types.Position{
						Commit:  checkpoint.CommitPosition,
						Prepare: checkpoint.PreparePosition,
					}

					req.channel <- &subscription.Event{
						CheckPointReached: &position,
					}
				}
			case *api.ReadResp_Event:
				{
					resolvedEvent := protoutils.GetResolvedEventFromProto(result.GetEvent())
					req.channel <- &subscription.Event{
						EventAppeared: &resolvedEvent,
					}
				}
			}
		}
	}()

	return &Subscription{
		client:  client,
		id:      id,
		inner:   inner,
		channel: channel,
		once:    once,
		cancel:  cancel,
	}
}

func (sub *Subscription) Id() string {
	return sub.id
}

func (sub *Subscription) Close() error {
	sub.once.Do(sub.cancel)
	return nil
}

func (sub *Subscription) Recv() *subscription.Event {
	channel := make(chan *subscription.Event)
	req := request{
		channel: channel,
	}

	sub.channel <- req
	resp := <-channel

	return resp
}

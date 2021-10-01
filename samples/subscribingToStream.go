package samples

import (
	"context"
	"time"

	"github.com/EventStore/EventStore-Client-Go/client"
	"github.com/EventStore/EventStore-Client-Go/client/filtering"
	"github.com/EventStore/EventStore-Client-Go/position"
)

func SubscribeToStream(db *client.Client) {
	// region subscribe-to-stream
	opts := client.SubscribeToStreamOptions{}

	stream, err := db.SubscribeToStream(context.Background(), "some-stream", client.SubscribeToStreamOptions{})

	if err != nil {
		panic(err)
	}

	defer stream.Close()

	for {
		event := stream.Recv()

		if event.EventAppeared != nil {
			// handles the event...
		}

		if event.SubscriptionDropped != nil {
			break
		}
	}
	// endregion subscribe-to-stream

	// region subscribe-to-stream-from-position
	opts.SetFromRevision(20)

	db.SubscribeToStream(context.Background(), "some-stream", opts)
	// endregion subscribe-to-stream-from-position

	// region subscribe-to-stream-live
	opts.SetFromEnd()

	db.SubscribeToStream(context.Background(), "some-stream", opts)
	// endregion subscribe-to-stream-live

	// region subscribe-to-stream-resolving-linktos
	opts.SetFromStart()
	opts.SetResolveLinks()

	db.SubscribeToStream(context.Background(), "$et-myEventType", opts)
	// endregion subscribe-to-stream-resolving-linktos

	// region subscribe-to-stream-subscription-dropped
	opts.SetFromStart()

	for {
		var stream *client.Subscription = nil

		if stream == nil {
			stream, err := db.SubscribeToStream(context.Background(), "some-stream", opts)

			if err != nil {
				time.Sleep(1 * time.Second)
			}

			for {
				event := stream.Recv()

				if event.SubscriptionDropped != nil {
					stream = nil
					break
				}

				if event.EventAppeared != nil {
					// handles the event...
					opts.SetFromRevision(event.EventAppeared.OriginalEvent().EventNumber)
				}
			}
		}
	}
	// endregion subscribe-to-stream-subscription-dropped
}

func SubscribeToAll(db *client.Client) {
	// region subscribe-to-all
	stream, err := db.SubscribeToAll(context.Background(), client.SubscribeToAllOptions{})

	if err != nil {
		panic(err)
	}

	defer stream.Close()

	for {
		event := stream.Recv()

		if event.EventAppeared != nil {
			// handles the event...
		}

		if event.SubscriptionDropped != nil {
			break
		}
	}
	// endregion subscribe-to-all

	// region subscribe-to-all-from-position
	opts := client.SubscribeToAllOptions{}
	opts.SetFromPosition(position.Position{
		Commit:  1_056,
		Prepare: 1_056,
	})

	db.SubscribeToAll(context.Background(), opts)
	// endregion subscribe-to-all-from-position

	// region subscribe-to-all-live
	opts.SetFromEnd()
	db.SubscribeToAll(context.Background(), opts)
	// endregion subscribe-to-all-live

	// region subscribe-to-all-subscription-dropped
	opts.SetFromStart()
	for {
		var stream *client.Subscription = nil

		if stream == nil {
			stream, err := db.SubscribeToAll(context.Background(), opts)

			if err != nil {
				time.Sleep(1 * time.Second)
			}

			for {
				event := stream.Recv()

				if event.SubscriptionDropped != nil {
					stream = nil
					break
				}

				if event.EventAppeared != nil {
					// handles the event...
					opts.SetFromPosition(event.EventAppeared.OriginalEvent().Position)
				}
			}
		}
	}
	// endregion subscribe-to-all-subscription-dropped
}

func SubscribeToFiltered(db *client.Client) {
	// region stream-prefix-filtered-subscription
	filter := filtering.SubscriptionFilterOptionsDefault(filtering.FilterOnStreamName().AddPrefixes("test-"))
	opts := client.SubscribeToAllOptions{}
	opts.SetFilter(filter)

	db.SubscribeToAll(context.Background(), opts)
	// endregion stream-prefix-filtered-subscription
	// region stream-regex-filtered-subscription
	filter = filtering.SubscriptionFilterOptionsDefault(filtering.FilterOnStreamName().Regex("/invoice-\\d\\d\\d/g"))
	// endregion stream-regex-filtered-subscription

}

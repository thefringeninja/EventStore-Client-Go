package samples

import (
	"context"
	"time"

	"github.com/EventStore/EventStore-Client-Go/client"
)

func SubscribeToStream(db *client.Client) {
	options := client.SubscribeToStreamOptions{}
	// region subscribe-to-stream
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
	db.SubscribeToStream(context.Background(), "some-stream", client.SubscribeToStreamOptions{
		From: client.Revision(20),
	})
	// endregion subscribe-to-stream-from-position

	// region subscribe-to-stream-live
	options = client.SubscribeToStreamOptions{
		From: client.End{},
	}

	db.SubscribeToStream(context.Background(), "some-stream", options)
	// endregion subscribe-to-stream-live

	// region subscribe-to-stream-resolving-linktos
	options = client.SubscribeToStreamOptions{
		From:         client.Start{},
		ResolveLinks: true,
	}

	db.SubscribeToStream(context.Background(), "$et-myEventType", options)
	// endregion subscribe-to-stream-resolving-linktos

	// region subscribe-to-stream-subscription-dropped
	options = client.SubscribeToStreamOptions{
		From: client.Start{},
	}

	for {

		stream, err := db.SubscribeToStream(context.Background(), "some-stream", options)

		if err != nil {
			time.Sleep(1 * time.Second)
			continue
		}

		for {
			event := stream.Recv()

			if event.SubscriptionDropped != nil {
				stream.Close()
				break
			}

			if event.EventAppeared != nil {
				// handles the event...
				options.From = client.Revision(event.EventAppeared.OriginalEvent().EventNumber)
			}
		}
	}
	// endregion subscribe-to-stream-subscription-dropped
}

func SubscribeToAll(db *client.Client) {
	options := client.SubscribeToAllOptions{}
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
	db.SubscribeToAll(context.Background(), client.SubscribeToAllOptions{
		From: client.Position{
			Commit:  1_056,
			Prepare: 1_056,
		},
	})
	// endregion subscribe-to-all-from-position

	// region subscribe-to-all-live
	db.SubscribeToAll(context.Background(), client.SubscribeToAllOptions{
		From: client.End{},
	})
	// endregion subscribe-to-all-live

	// region subscribe-to-all-subscription-dropped
	options = client.SubscribeToAllOptions{
		From: client.Start{},
	}

	for {
		stream, err := db.SubscribeToAll(context.Background(), options)

		if err != nil {
			time.Sleep(1 * time.Second)
			continue
		}

		for {
			event := stream.Recv()

			if event.SubscriptionDropped != nil {
				stream.Close()
				break
			}

			if event.EventAppeared != nil {
				// handles the event...
				options.From = event.EventAppeared.OriginalEvent().Position
			}
		}
	}
	// endregion subscribe-to-all-subscription-dropped
}

func SubscribeToFiltered(db *client.Client) {
	// region stream-prefix-filtered-subscription
	db.SubscribeToAll(context.Background(), client.SubscribeToAllOptions{
		Filter: &client.SubscriptionFilter{
			Type:     client.StreamFilterType,
			Prefixes: []string{"test-"},
		},
	})
	// endregion stream-prefix-filtered-subscription
	// region stream-regex-filtered-subscription
	db.SubscribeToAll(context.Background(), client.SubscribeToAllOptions{
		Filter: &client.SubscriptionFilter{
			Type:  client.StreamFilterType,
			Regex: "/invoice-\\d\\d\\d/g",
		},
	})
	// endregion stream-regex-filtered-subscription

}

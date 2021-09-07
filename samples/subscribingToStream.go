package samples

import (
	"context"
	"time"

	"github.com/EventStore/EventStore-Client-Go/client"
	"github.com/EventStore/EventStore-Client-Go/client/filtering"
	"github.com/EventStore/EventStore-Client-Go/options"
	"github.com/EventStore/EventStore-Client-Go/position"
	"github.com/EventStore/EventStore-Client-Go/stream_position"
)

func SubscribeToStream(db *client.Client) {
	// region subscribe-to-stream
	opts := options.SubscribeToStreamOptionsDefault()

	stream, err := db.SubscribeToStream(context.Background(), "some-stream", &opts)

	if err != nil {
		panic(err)
	}

	defer stream.Close()

	for {
		event := stream.Recv()

		if event.EventAppeared != nil {
			// handles the event...
		}

		if event.Dropped != nil {
			break
		}
	}
	// endregion subscribe-to-stream

	// region subscribe-to-stream-from-position
	opts = options.SubscribeToStreamOptionsDefault().Position(stream_position.Revision(20))

	db.SubscribeToStream(context.Background(), "some-stream", &opts)
	// endregion subscribe-to-stream-from-position

	// region subscribe-to-stream-live
	opts = options.SubscribeToStreamOptionsDefault().Position(stream_position.End())

	db.SubscribeToStream(context.Background(), "some-stream", &opts)
	// endregion subscribe-to-stream-live

	// region subscribe-to-stream-resolving-linktos
	opts = options.SubscribeToStreamOptionsDefault().Position(stream_position.Start()).ResolveLinks()

	db.SubscribeToStream(context.Background(), "$et-myEventType", &opts)
	// endregion subscribe-to-stream-resolving-linktos

	// region subscribe-to-stream-subscription-dropped
	var offset stream_position.StreamPosition = stream_position.Start()
	for {
		var stream *client.Subscription = nil

		if stream == nil {
			opts := options.SubscribeToStreamOptionsDefault().Position(offset)
			stream, err := db.SubscribeToStream(context.Background(), "some-stream", &opts)

			if err != nil {
				 time.Sleep(1 * time.Second)
			}

			for {
				event := stream.Recv()

				if event.Dropped != nil {
					stream = nil
					break
				}

				if event.EventAppeared != nil {
					// handles the event...
					offset = stream_position.Revision(event.EventAppeared.GetOriginalEvent().EventNumber)
				}
			}
		}
	}
	// endregion subscribe-to-stream-subscription-dropped
}

func SubscribeToAll(db *client.Client) {
	// region subscribe-to-all
	opts := options.SubscribeToAllOptionsDefault()
	stream, err := db.SubscribeToAll(context.Background(), &opts)

	if err != nil {
		panic(err)
	}

	defer stream.Close()

	for {
		event := stream.Recv()

		if event.EventAppeared != nil {
			// handles the event...
		}

		if event.Dropped != nil {
			break
		}
	}
	// endregion subscribe-to-all

	// region subscribe-to-all-from-position
	opts = options.SubscribeToAllOptionsDefault().Position(stream_position.Position(position.Position{
		Commit: 1_056,
		Prepare: 1_056,
	}))

	db.SubscribeToAll(context.Background(), &opts)
	// endregion subscribe-to-all-from-position

	// region suscribe-to-all-live
	opts = options.SubscribeToAllOptionsDefault().Position(stream_position.End())
	db.SubscribeToAll(context.Background(), &opts)
	// endregion suscribe-to-all-live

	// region subscribe-to-all-subscription-dropped
	var offset stream_position.AllStreamPosition = stream_position.Start()
	for {
		var stream *client.Subscription = nil

		if stream == nil {
			opts := options.SubscribeToAllOptionsDefault().Position(offset)
			stream, err := db.SubscribeToAll(context.Background(), &opts)

			if err != nil {
				 time.Sleep(1 * time.Second)
			}

			for {
				event := stream.Recv()

				if event.Dropped != nil {
					stream = nil
					break
				}

				if event.EventAppeared != nil {
					// handles the event...
					offset = stream_position.Position(event.EventAppeared.GetOriginalEvent().Position)
				}
			}
		}
	}
	// endregion subscribe-to-all-subscription-dropped
}

func SubscribeToFiltered(db *client.Client) {
	// region stream-prefix-filtered-subscription
	filter := filtering.SubscriptionFilterOptionsDefault(filtering.FilterOnStreamName().AddPrefixes("test-"))
	opts := options.SubscribeToAllOptionsDefault().Filter(filter)

	db.SubscribeToAll(context.Background(), &opts)
	// endregion stream-prefix-filtered-subscription
	// region stream-regex-filtered-subscription
	filter = filtering.SubscriptionFilterOptionsDefault(filtering.FilterOnStreamName().Regex("/invoice-\\d\\d\\d/g"))
	// endregion stream-regex-filtered-subscription

}

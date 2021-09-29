package samples

import (
	"context"
	"fmt"

	"github.com/EventStore/EventStore-Client-Go/client"
	"github.com/EventStore/EventStore-Client-Go/client/filtering"
	"github.com/EventStore/EventStore-Client-Go/options"
)

func ExcludeSystemEvents(db *client.Client) {
	// region exclude-system
	filter := filtering.SubscriptionFilterOptionsDefault(filtering.FilterOnEventType().Regex("/^[^\\$].*/"))
	opts := options.SubscribeToAllOptions{}
	opts.SetDefaults()
	opts.SetFilter(filter)

	sub, err := db.SubscribeToAll(context.Background(), opts)

	if err != nil {
		panic(err)
	}

	defer sub.Close()

	for {
		event := sub.Recv()

		if event.EventAppeared != nil {
			streamId := event.EventAppeared.GetOriginalEvent().StreamID
			revision := event.EventAppeared.GetOriginalEvent().EventNumber

			fmt.Printf("received event %v@%v", revision, streamId)
		}

		if event.Dropped != nil {
			break
		}
	}

	// endregion exclude-system
}

func EventTypePrefix(db *client.Client) {
	// region event-type-prefix
	filter := filtering.SubscriptionFilterOptionsDefault(filtering.FilterOnEventType().AddPrefixes("customer-"))
	opts := options.SubscribeToAllOptions{}
	opts.SetDefaults()
	opts.SetFilter(filter)

	sub, err := db.SubscribeToAll(context.Background(), opts)

	if err != nil {
		panic(err)
	}

	defer sub.Close()

	for {
		event := sub.Recv()

		if event.EventAppeared != nil {
			streamId := event.EventAppeared.GetOriginalEvent().StreamID
			revision := event.EventAppeared.GetOriginalEvent().EventNumber

			fmt.Printf("received event %v@%v", revision, streamId)
		}

		if event.Dropped != nil {
			break
		}
	}

	// endregion event-type-prefix
}

func EventTypeRegex(db *client.Client) {
	// region event-type-regex
	filter := filtering.SubscriptionFilterOptionsDefault(filtering.FilterOnEventType().Regex("^user|^company"))
	opts := options.SubscribeToAllOptions{}
	opts.SetDefaults()
	opts.SetFilter(filter)

	sub, err := db.SubscribeToAll(context.Background(), opts)

	if err != nil {
		panic(err)
	}

	defer sub.Close()

	for {
		event := sub.Recv()

		if event.EventAppeared != nil {
			streamId := event.EventAppeared.GetOriginalEvent().StreamID
			revision := event.EventAppeared.GetOriginalEvent().EventNumber

			fmt.Printf("received event %v@%v", revision, streamId)
		}

		if event.Dropped != nil {
			break
		}
	}

	// endregion event-type-regex
}

func StreamPrefix(db *client.Client) {
	// region stream-prefix
	filter := filtering.SubscriptionFilterOptionsDefault(filtering.FilterOnStreamName().AddPrefixes("user-"))
	opts := options.SubscribeToAllOptions{}
	opts.SetDefaults()
	opts.SetFilter(filter)

	sub, err := db.SubscribeToAll(context.Background(), opts)

	if err != nil {
		panic(err)
	}

	defer sub.Close()

	for {
		event := sub.Recv()

		if event.EventAppeared != nil {
			streamId := event.EventAppeared.GetOriginalEvent().StreamID
			revision := event.EventAppeared.GetOriginalEvent().EventNumber

			fmt.Printf("received event %v@%v", revision, streamId)
		}

		if event.Dropped != nil {
			break
		}
	}

	// endregion stream-prefix
}

func StreamRegex(db *client.Client) {
	// region stream-regex
	filter := filtering.SubscriptionFilterOptionsDefault(filtering.FilterOnStreamName().Regex("^user|^company"))
	opts := options.SubscribeToAllOptions{}
	opts.SetDefaults()
	opts.SetFilter(filter)

	sub, err := db.SubscribeToAll(context.Background(), opts)

	if err != nil {
		panic(err)
	}

	defer sub.Close()

	for {
		event := sub.Recv()

		if event.EventAppeared != nil {
			streamId := event.EventAppeared.GetOriginalEvent().StreamID
			revision := event.EventAppeared.GetOriginalEvent().EventNumber

			fmt.Printf("received event %v@%v", revision, streamId)
		}

		if event.Dropped != nil {
			break
		}
	}

	// endregion stream-regex
}

func CheckpointCallbackWithInterval(db *client.Client) {
	// region checkpoint-with-interval
	filter := filtering.SubscriptionFilterOptionsDefault(filtering.FilterOnEventType().Regex("/^[^\\$].*/"))
	opts := options.SubscribeToAllOptions{}
	opts.SetDefaults()
	opts.SetFilter(filter)

	sub, err := db.SubscribeToAll(context.Background(), opts)

	if err != nil {
		panic(err)
	}

	defer sub.Close()

	for {
		event := sub.Recv()

		if event.EventAppeared != nil {
			streamId := event.EventAppeared.GetOriginalEvent().StreamID
			revision := event.EventAppeared.GetOriginalEvent().EventNumber

			fmt.Printf("received event %v@%v", revision, streamId)
		}

		if event.CheckPointReached != nil {
			fmt.Printf("checkpoint taken at %v", event.CheckPointReached.Prepare)
		}

		if event.Dropped != nil {
			break
		}
	}

	// endregion checkpoint-with-interval
}

package samples

import (
	"context"
	"fmt"

	"github.com/EventStore/EventStore-Client-Go/client"
	"github.com/EventStore/EventStore-Client-Go/client/filtering"
)

func ExcludeSystemEvents(db *client.Client) {
	// region exclude-system
	filter := filtering.NewFilterOnEventType()
	filter.SetRegex("/^[^\\$].*/")
	opts := client.SubscribeToAllOptions{}
	opts.SetFilter(filter)

	sub, err := db.SubscribeToAll(context.Background(), opts)

	if err != nil {
		panic(err)
	}

	defer sub.Close()

	for {
		event := sub.Recv()

		if event.EventAppeared != nil {
			streamId := event.EventAppeared.OriginalEvent().StreamID
			revision := event.EventAppeared.OriginalEvent().EventNumber

			fmt.Printf("received event %v@%v", revision, streamId)
		}

		if event.SubscriptionDropped != nil {
			break
		}
	}

	// endregion exclude-system
}

func EventTypePrefix(db *client.Client) {
	// region event-type-prefix
	filter := filtering.NewFilterOnEventType()
	filter.AddPrefixes("customer-")
	opts := client.SubscribeToAllOptions{}
	opts.SetFilter(filter)

	sub, err := db.SubscribeToAll(context.Background(), opts)

	if err != nil {
		panic(err)
	}

	defer sub.Close()

	for {
		event := sub.Recv()

		if event.EventAppeared != nil {
			streamId := event.EventAppeared.OriginalEvent().StreamID
			revision := event.EventAppeared.OriginalEvent().EventNumber

			fmt.Printf("received event %v@%v", revision, streamId)
		}

		if event.SubscriptionDropped != nil {
			break
		}
	}

	// endregion event-type-prefix
}

func EventTypeRegex(db *client.Client) {
	// region event-type-regex
	filter := filtering.NewFilterOnEventType()
	filter.SetRegex("^user|^company")
	opts := client.SubscribeToAllOptions{}
	opts.SetFilter(filter)

	sub, err := db.SubscribeToAll(context.Background(), opts)

	if err != nil {
		panic(err)
	}

	defer sub.Close()

	for {
		event := sub.Recv()

		if event.EventAppeared != nil {
			streamId := event.EventAppeared.OriginalEvent().StreamID
			revision := event.EventAppeared.OriginalEvent().EventNumber

			fmt.Printf("received event %v@%v", revision, streamId)
		}

		if event.SubscriptionDropped != nil {
			break
		}
	}

	// endregion event-type-regex
}

func StreamPrefix(db *client.Client) {
	// region stream-prefix
	filter := filtering.NewFilterOnStreamName()
	filter.AddPrefixes("user-")
	opts := client.SubscribeToAllOptions{}
	opts.SetFilter(filter)

	sub, err := db.SubscribeToAll(context.Background(), opts)

	if err != nil {
		panic(err)
	}

	defer sub.Close()

	for {
		event := sub.Recv()

		if event.EventAppeared != nil {
			streamId := event.EventAppeared.OriginalEvent().StreamID
			revision := event.EventAppeared.OriginalEvent().EventNumber

			fmt.Printf("received event %v@%v", revision, streamId)
		}

		if event.SubscriptionDropped != nil {
			break
		}
	}

	// endregion stream-prefix
}

func StreamRegex(db *client.Client) {
	// region stream-regex
	filter := filtering.NewFilterOnStreamName()
	filter.SetRegex("^user|^company")
	opts := client.SubscribeToAllOptions{}
	opts.SetFilter(filter)

	sub, err := db.SubscribeToAll(context.Background(), opts)

	if err != nil {
		panic(err)
	}

	defer sub.Close()

	for {
		event := sub.Recv()

		if event.EventAppeared != nil {
			streamId := event.EventAppeared.OriginalEvent().StreamID
			revision := event.EventAppeared.OriginalEvent().EventNumber

			fmt.Printf("received event %v@%v", revision, streamId)
		}

		if event.SubscriptionDropped != nil {
			break
		}
	}

	// endregion stream-regex
}

func CheckpointCallbackWithInterval(db *client.Client) {
	// region checkpoint-with-interval
	filter := filtering.NewFilterOnEventType()
	filter.SetRegex("/^[^\\$].*/")
	opts := client.SubscribeToAllOptions{}
	opts.SetFilter(filter)

	sub, err := db.SubscribeToAll(context.Background(), opts)

	if err != nil {
		panic(err)
	}

	defer sub.Close()

	for {
		event := sub.Recv()

		if event.EventAppeared != nil {
			streamId := event.EventAppeared.OriginalEvent().StreamID
			revision := event.EventAppeared.OriginalEvent().EventNumber

			fmt.Printf("received event %v@%v", revision, streamId)
		}

		if event.CheckPointReached != nil {
			fmt.Printf("checkpoint taken at %v", event.CheckPointReached.Prepare)
		}

		if event.SubscriptionDropped != nil {
			break
		}
	}

	// endregion checkpoint-with-interval
}

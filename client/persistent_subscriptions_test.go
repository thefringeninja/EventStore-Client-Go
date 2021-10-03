package client_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/EventStore/EventStore-Client-Go/messages"
	"github.com/EventStore/EventStore-Client-Go/types"

	"github.com/EventStore/EventStore-Client-Go/client"
	"github.com/stretchr/testify/require"
)

func Test_CreatePersistentStreamSubscription(t *testing.T) {
	containerInstance, clientInstance := initializeContainerAndClient(t)
	defer func() {
		err := clientInstance.Close()
		require.NoError(t, err)
	}()
	defer containerInstance.Close()

	streamID := "someStream"
	pushEventToStream(t, clientInstance, streamID)

	err := clientInstance.CreatePersistentSubscription(
		context.Background(),
		streamID,
		"Group 1",
		client.PersistentStreamSubscriptionOptions{},
	)

	require.NoError(t, err)
}

func Test_CreatePersistentStreamSubscription_MessageTimeoutZero(t *testing.T) {
	containerInstance, clientInstance := initializeContainerAndClient(t)
	defer func() {
		err := clientInstance.Close()
		require.NoError(t, err)
	}()
	defer containerInstance.Close()

	streamID := "someStream"
	pushEventToStream(t, clientInstance, streamID)

	settings := types.SubscriptionSettingsDefault()
	settings.MessageTimeoutInMs = 0

	options := client.PersistentStreamSubscriptionOptions{}
	options.SetSettings(settings)
	err := clientInstance.CreatePersistentSubscription(
		context.Background(),
		streamID,
		"Group 1",
		options,
	)

	require.NoError(t, err)
}

func Test_CreatePersistentStreamSubscription_StreamNotExits(t *testing.T) {
	containerInstance, clientInstance := initializeContainerAndClient(t)
	defer func() {
		err := clientInstance.Close()
		require.NoError(t, err)
	}()
	defer containerInstance.Close()

	streamID := "someStream"

	err := clientInstance.CreatePersistentSubscription(
		context.Background(),
		streamID,
		"Group 1",
		client.PersistentStreamSubscriptionOptions{},
	)

	require.NoError(t, err)
}

func Test_CreatePersistentStreamSubscription_FailsIfAlreadyExists(t *testing.T) {
	containerInstance, clientInstance := initializeContainerAndClient(t)
	defer func() {
		err := clientInstance.Close()
		require.NoError(t, err)
	}()
	defer containerInstance.Close()

	streamID := "someStream"
	pushEventToStream(t, clientInstance, streamID)

	err := clientInstance.CreatePersistentSubscription(
		context.Background(),
		streamID,
		"Group 1",
		client.PersistentStreamSubscriptionOptions{},
	)

	require.NoError(t, err)

	err = clientInstance.CreatePersistentSubscription(
		context.Background(),
		streamID,
		"Group 1",
		client.PersistentStreamSubscriptionOptions{},
	)

	require.Error(t, err)
}

func Test_CreatePersistentStreamSubscription_AfterDeleting(t *testing.T) {
	containerInstance, clientInstance := initializeContainerAndClient(t)
	defer func() {
		err := clientInstance.Close()
		require.NoError(t, err)
	}()
	defer containerInstance.Close()

	streamID := "someStream"
	pushEventToStream(t, clientInstance, streamID)

	err := clientInstance.CreatePersistentSubscription(
		context.Background(),
		streamID,
		"Group 1",
		client.PersistentStreamSubscriptionOptions{},
	)

	require.NoError(t, err)

	err = clientInstance.DeletePersistentSubscription(context.Background(), streamID, "Group 1")

	require.NoError(t, err)

	err = clientInstance.CreatePersistentSubscription(
		context.Background(),
		streamID,
		"Group 1",
		client.PersistentStreamSubscriptionOptions{},
	)

	require.NoError(t, err)
}

func Test_UpdatePersistentStreamSubscription(t *testing.T) {
	containerInstance, clientInstance := initializeContainerAndClient(t)
	defer func() {
		err := clientInstance.Close()
		require.NoError(t, err)
	}()
	defer containerInstance.Close()

	streamID := "someStream"
	pushEventToStream(t, clientInstance, streamID)

	err := clientInstance.CreatePersistentSubscription(
		context.Background(),
		streamID,
		"Group 1",
		client.PersistentStreamSubscriptionOptions{},
	)

	require.NoError(t, err)

	options := client.PersistentStreamSubscriptionOptions{}
	settings := types.SubscriptionSettingsDefault()

	settings.HistoryBufferSize = settings.HistoryBufferSize + 1
	settings.NamedConsumerStrategy = types.ConsumerStrategy_DispatchToSingle
	settings.MaxSubscriberCount = settings.MaxSubscriberCount + 1
	settings.ReadBatchSize = settings.ReadBatchSize + 1
	settings.CheckpointAfterInMs = settings.CheckpointAfterInMs + 1
	settings.MaxCheckpointCount = settings.MaxCheckpointCount + 1
	settings.MinCheckpointCount = settings.MinCheckpointCount + 1
	settings.LiveBufferSize = settings.LiveBufferSize + 1
	settings.MaxRetryCount = settings.MaxRetryCount + 1
	settings.MessageTimeoutInMs = settings.MessageTimeoutInMs + 1
	settings.ExtraStatistics = !settings.ExtraStatistics
	settings.ResolveLinks = !settings.ResolveLinks

	options.SetSettings(settings)
	err = clientInstance.UpdatePersistentStreamSubscription(context.Background(), streamID, "Group 1", options)

	require.NoError(t, err)
}

func Test_UpdatePersistentStreamSubscription_ErrIfSubscriptionDoesNotExist(t *testing.T) {
	containerInstance, clientInstance := initializeContainerAndClient(t)
	defer func() {
		err := clientInstance.Close()
		require.NoError(t, err)
	}()
	defer containerInstance.Close()

	streamID := "someStream"

	err := clientInstance.UpdatePersistentStreamSubscription(context.Background(), streamID, "Group 1", client.PersistentStreamSubscriptionOptions{})

	require.Error(t, err)
}

func Test_DeletePersistentStreamSubscription(t *testing.T) {
	containerInstance, clientInstance := initializeContainerAndClient(t)
	defer func() {
		err := clientInstance.Close()
		require.NoError(t, err)
	}()
	defer containerInstance.Close()

	streamID := "someStream"
	pushEventToStream(t, clientInstance, streamID)

	err := clientInstance.CreatePersistentSubscription(
		context.Background(),
		streamID,
		"Group 1",
		client.PersistentStreamSubscriptionOptions{},
	)

	require.NoError(t, err)

	err = clientInstance.DeletePersistentSubscription(
		context.Background(),
		streamID,
		"Group 1",
	)

	require.NoError(t, err)
}

func Test_DeletePersistentSubscription_ErrIfSubscriptionDoesNotExist(t *testing.T) {
	containerInstance, clientInstance := initializeContainerAndClient(t)
	defer func() {
		err := clientInstance.Close()
		require.NoError(t, err)
	}()
	defer containerInstance.Close()

	err := clientInstance.DeletePersistentSubscription(
		context.Background(),
		"a",
		"a",
	)

	require.Error(t, err)
}

func initializeContainerAndClient(t *testing.T) (*Container, *client.Client) {
	container := GetEmptyDatabase()
	clientInstance := CreateTestClient(container, t)
	return container, clientInstance
}

func pushEventToStream(t *testing.T, clientInstance *client.Client, streamID string) {
	testEvent := createTestEvent()
	pushEventsToStream(t, clientInstance, streamID, []messages.ProposedEvent{testEvent})
}

func pushEventsToStream(t *testing.T,
	clientInstance *client.Client,
	streamID string,
	events []messages.ProposedEvent) {

	opts := client.AppendToStreamOptions{}
	opts.SetExpectNoStream()
	_, err := clientInstance.AppendToStream(context.Background(), streamID, opts, events...)

	require.NoError(t, err)
}

func TestPersistentSubscriptionClosing(t *testing.T) {
	container := GetPrePopulatedDatabase()
	defer container.Close()
	db := CreateTestClient(container, t)
	defer db.Close()

	streamID := "dataset20M-0"
	groupName := "Group 1"

	opts := client.PersistentStreamSubscriptionOptions{}
	opts.SetFromStart()

	err := db.CreatePersistentSubscription(context.Background(), streamID, groupName, opts)

	require.NoError(t, err)

	var receivedEvents sync.WaitGroup
	var droppedEvent sync.WaitGroup

	optsC := client.ConnectToPersistentSubscriptionOptions{}
	optsC.SetBatchSize(2)
	subscription, err := db.ConnectToPersistentSubscription(
		context.Background(), streamID, groupName, optsC)

	require.NoError(t, err)

	go func() {
		current := 1

		for {
			subEvent := subscription.Recv()

			if subEvent.EventAppeared != nil {
				if current <= 10 {
					receivedEvents.Done()
					current++
				}

				subscription.Ack(subEvent.EventAppeared)

				continue
			}

			if subEvent.SubscriptionDropped != nil {
				droppedEvent.Done()
				break
			}
		}
	}()

	require.NoError(t, err)
	receivedEvents.Add(10)
	droppedEvent.Add(1)
	timedOut := waitWithTimeout(&receivedEvents, time.Duration(5)*time.Second)
	require.False(t, timedOut, "Timed out waiting for initial set of events")
	subscription.Close()
	timedOut = waitWithTimeout(&droppedEvent, time.Duration(5)*time.Second)
	require.False(t, timedOut, "Timed out waiting for dropped event")
}

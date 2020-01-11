package client_test

import (
	"context"
	"testing"
	"time"

	direction "github.com/eventstore/EventStore-Client-Go/pkg/direction"
	position "github.com/eventstore/EventStore-Client-Go/pkg/position"
	"github.com/gofrs/uuid"
	"github.com/stretchr/testify/assert"
)

func TestReadAllEventsForwardsFromZeroPosition(t *testing.T) {
	container := GetPrePopulatedDatabase()
	defer container.Close()

	client := CreateTestClient(container, t)
	defer client.Close()

	context, cancel := context.WithTimeout(context.Background(), time.Duration(5)*time.Second)
	defer cancel()

	numberOfEvents := int32(10)

	readResult, err := client.ReadAllEvents(context, direction.Forwards, position.StartPosition(), numberOfEvents, true)

	if err != nil {
		t.Fatalf("Unexpected failure %+v", err)
	}

	assert.Equal(t, numberOfEvents, int32(len(readResult.Events)), "Expected the correct number of messages to be returned")
	expectedEventID, _ := uuid.FromString("5ed0b2c9-dde9-4312-941e-19c494536f37")
	assert.Equal(t, expectedEventID, readResult.Events[0].EventID)
	assert.Equal(t, "$metadata", readResult.Events[0].EventType)
	assert.Equal(t, "$$$stats-0.0.0.0:2113", readResult.Events[0].StreamID)
	assert.Equal(t, uint64(0), readResult.Events[0].StreamRevision)
	expectedCreated, _ := time.Parse(time.RFC3339, "2020-01-12T18:14:13.990951Z")
	assert.Equal(t, expectedCreated, readResult.Events[0].CreatedDate)
	assert.Equal(t, position.NewPosition(167, 167), readResult.Events[0].Position)
	assert.Equal(t, true, readResult.Events[0].IsJSON)

	expectedEventID, _ = uuid.FromString("4936a85f-e6cb-4a72-9007-ceed0c8a56e7")
	assert.Equal(t, expectedEventID, readResult.Events[9].EventID)
	assert.Equal(t, "$metadata", readResult.Events[9].EventType)
	assert.Equal(t, "$$$projections-$0c745883dffc440e89dcf4f511e81101", readResult.Events[9].StreamID)
	assert.Equal(t, uint64(0), readResult.Events[9].StreamRevision)
	expectedCreated, _ = time.Parse(time.RFC3339, "2020-01-12T18:14:14.108422Z")
	assert.Equal(t, expectedCreated, readResult.Events[9].CreatedDate)
	assert.Equal(t, position.NewPosition(1788, 1788), readResult.Events[9].Position)
	assert.Equal(t, true, readResult.Events[9].IsJSON)
}

func TestReadAllEventsForwardsFromNonZeroPosition(t *testing.T) {
	container := GetPrePopulatedDatabase()
	defer container.Close()

	client := CreateTestClient(container, t)
	defer client.Close()

	context, cancel := context.WithTimeout(context.Background(), time.Duration(5)*time.Second)
	defer cancel()

	numberOfEvents := int32(10)

	readResult, err := client.ReadAllEvents(context, direction.Forwards, position.Position{Commit: 1788, Prepare: 1788}, numberOfEvents, true)

	if err != nil {
		t.Fatalf("Unexpected failure %+v", err)
	}

	assert.Equal(t, numberOfEvents, int32(len(readResult.Events)), "Expected the correct number of messages to be returned")
	expectedEventID, _ := uuid.FromString("4936a85f-e6cb-4a72-9007-ceed0c8a56e7")
	assert.Equal(t, expectedEventID, readResult.Events[0].EventID)
	assert.Equal(t, "$metadata", readResult.Events[0].EventType)
	assert.Equal(t, "$$$projections-$0c745883dffc440e89dcf4f511e81101", readResult.Events[0].StreamID)
	assert.Equal(t, uint64(0), readResult.Events[0].StreamRevision)
	expectedCreated, _ := time.Parse(time.RFC3339, "2020-01-12T18:14:14.108422Z")
	assert.Equal(t, expectedCreated, readResult.Events[0].CreatedDate)
	assert.Equal(t, position.NewPosition(1788, 1788), readResult.Events[0].Position)
	assert.Equal(t, true, readResult.Events[0].IsJSON)

	expectedEventID, _ = uuid.FromString("c4bde754-dc19-4835-9005-1e82002ecc10")
	assert.Equal(t, expectedEventID, readResult.Events[9].EventID)
	assert.Equal(t, "$ProjectionsInitialized", readResult.Events[9].EventType)
	assert.Equal(t, "$projections-$all", readResult.Events[9].StreamID)
	assert.Equal(t, uint64(0), readResult.Events[9].StreamRevision)
	expectedCreated, _ = time.Parse(time.RFC3339, "2020-01-12T18:14:14.158068Z")
	assert.Equal(t, expectedCreated, readResult.Events[9].CreatedDate)
	assert.Equal(t, position.NewPosition(3256, 3256), readResult.Events[9].Position)
	assert.Equal(t, false, readResult.Events[9].IsJSON)
}

func TestReadAllEventsBackwardsFromZeroPosition(t *testing.T) {
	container := GetPrePopulatedDatabase()
	defer container.Close()

	client := CreateTestClient(container, t)
	defer client.Close()

	context, cancel := context.WithTimeout(context.Background(), time.Duration(5)*time.Second)
	defer cancel()

	numberOfEvents := int32(10)

	readResult, err := client.ReadAllEvents(context, direction.Backwards, position.EndPosition(), numberOfEvents, true)

	if err != nil {
		t.Fatalf("Unexpected failure %+v", err)
	}

	assert.Equal(t, numberOfEvents, int32(len(readResult.Events)), "Expected the correct number of messages to be returned")
	expectedEventID, _ := uuid.FromString("01b58e8c-260c-4f77-a93d-c92edcc255c1")
	assert.Equal(t, expectedEventID, readResult.Events[0].EventID)
	assert.Equal(t, "$statsCollected", readResult.Events[0].EventType)
	assert.Equal(t, "$stats-0.0.0.0:2113", readResult.Events[0].StreamID)
	assert.Equal(t, uint64(11), readResult.Events[0].StreamRevision)
	expectedCreated, _ := time.Parse(time.RFC3339, "2020-01-12T18:20:14.583749Z")
	assert.Equal(t, expectedCreated, readResult.Events[0].CreatedDate)
	assert.Equal(t, position.NewPosition(20492574, 20492574), readResult.Events[0].Position)
	assert.Equal(t, true, readResult.Events[0].IsJSON)
}

func TestReadAllEventsBackwardsFromNonZeroPosition(t *testing.T) {
	container := GetPrePopulatedDatabase()
	defer container.Close()

	client := CreateTestClient(container, t)
	defer client.Close()

	context, cancel := context.WithTimeout(context.Background(), time.Duration(5)*time.Second)
	defer cancel()

	numberOfEvents := int32(10)

	readResult, err := client.ReadAllEvents(context, direction.Backwards, position.Position{Commit: 3386, Prepare: 3386}, numberOfEvents, true)

	if err != nil {
		t.Fatalf("Unexpected failure %+v", err)
	}

	assert.Equal(t, numberOfEvents, int32(len(readResult.Events)), "Expected the correct number of messages to be returned")

	expectedEventID, _ := uuid.FromString("c4bde754-dc19-4835-9005-1e82002ecc10")
	assert.Equal(t, expectedEventID, readResult.Events[0].EventID)
	assert.Equal(t, "$ProjectionsInitialized", readResult.Events[0].EventType)
	assert.Equal(t, "$projections-$all", readResult.Events[0].StreamID)
	assert.Equal(t, uint64(0), readResult.Events[0].StreamRevision)
	expectedCreated, _ := time.Parse(time.RFC3339, "2020-01-12T18:14:14.158068Z")
	assert.Equal(t, expectedCreated, readResult.Events[0].CreatedDate)
	assert.Equal(t, position.NewPosition(3256, 3256), readResult.Events[0].Position)
	assert.Equal(t, false, readResult.Events[0].IsJSON)

	expectedEventID, _ = uuid.FromString("4936a85f-e6cb-4a72-9007-ceed0c8a56e7")
	assert.Equal(t, expectedEventID, readResult.Events[9].EventID)
	assert.Equal(t, "$metadata", readResult.Events[9].EventType)
	assert.Equal(t, "$$$projections-$0c745883dffc440e89dcf4f511e81101", readResult.Events[9].StreamID)
	assert.Equal(t, uint64(0), readResult.Events[9].StreamRevision)
	expectedCreated, _ = time.Parse(time.RFC3339, "2020-01-12T18:14:14.108422Z")
	assert.Equal(t, expectedCreated, readResult.Events[9].CreatedDate)
	assert.Equal(t, position.NewPosition(1788, 1788), readResult.Events[9].Position)
	assert.Equal(t, true, readResult.Events[9].IsJSON)
}

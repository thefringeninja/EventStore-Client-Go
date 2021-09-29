package client_test

import (
	"context"
	"testing"

	"github.com/EventStore/EventStore-Client-Go/options"
	"github.com/stretchr/testify/assert"
)

func Test_NotLeaderExceptionButWorkAfterRetry(t *testing.T) {
	ctx := context.Background()

	// We purposely connect to a follower node so we can trigger on not leader exception.
	client := CreateClient("esdb://admin:changeit@localhost:2111,localhost:2112,localhost:2113?nodepreference=follower&tlsverifycert=false", t)
	options := options.PersistentStreamSubscriptionOptions{}
	options.SetDefaults()

	err := client.CreatePersistentSubscription(ctx, "myfoobar_123456", "a_group", options)

	assert.NotNil(t, err)

	// It should work now as the client automatically reconnected to the leader node.
	err = client.CreatePersistentSubscription(ctx, "myfoobar_123456", "a_group", options)

	if err != nil {
		t.Fatalf("Failed to create persistent subscription: %v", err)
	}

	assert.Nil(t, err)
}

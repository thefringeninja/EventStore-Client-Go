package client_test

import (
	"fmt"
	"log"
	"net/http"
	"testing"

	"github.com/eventstore/EventStore-Client-Go/pkg/client"
	"github.com/ory/dockertest"
	dc "github.com/ory/dockertest/docker"
)

// Container ...
type Container struct {
	Endpoint string
	Resource *dockertest.Resource
}

func (container *Container) Close() {
	container.Resource.Close()
}

func GetEmptyDatabase() *Container {
	options := &dockertest.RunOptions{
		Repository: "docker.pkg.github.com/eventstore/eventstore-client-grpc-testdata/eventstore-client-grpc-testdata",
		Tag:        "6.0.0-preview1.0.1869-buster-slim",
		PortBindings: map[dc.Port][]dc.PortBinding{
			"2113": []dc.PortBinding{{HostPort: "2113"}},
		},
		ExposedPorts: []string{"2113"},
		Env:          []string{"EVENTSTORE_DEV=true"},
	}

	return getDatabase(options)
}

func GetPrePopulatedDatabase() *Container {
	options := &dockertest.RunOptions{
		Repository: "docker.pkg.github.com/eventstore/eventstore-client-grpc-testdata/eventstore-client-grpc-testdata",
		Tag:        "6.0.0-preview1.0.1869-buster-slim",
		PortBindings: map[dc.Port][]dc.PortBinding{
			"2113": []dc.PortBinding{{HostPort: "2113"}},
		},
		ExposedPorts: []string{"2113"},
		Env:          []string{"EVENTSTORE_DEV=true", "EVENTSTORE_DB=/data/integration-tests", "EVENTSTORE_MEM_DB=false"},
	}
	return getDatabase(options)
}

func getDatabase(options *dockertest.RunOptions) *Container {
	pool, err := dockertest.NewPool("")
	if err != nil {
		log.Fatalf("Could not connect to docker: %s", err)
	}

	resource, err := pool.RunWithOptions(options)
	if err != nil {
		log.Fatalf("Could not start resource: %s", err)
	}

	endpoint := fmt.Sprintf("localhost:%s", resource.GetPort("2113/tcp"))

	pool.Retry(func() error {
		healthCheckEndpoint := fmt.Sprintf("https://%s/health/live", endpoint)
		_, err := http.Get(healthCheckEndpoint)
		return err
	})
	return &Container{
		Endpoint: endpoint,
		Resource: resource,
	}
}

func CreateTestClient(container *Container, t *testing.T) *client.Client {
	config := client.NewConfiguration()
	config.Address = container.Endpoint
	config.Username = "admin"
	config.Password = "changeit"
	config.SkipCertificateVerification = true

	client, err := client.NewClient(config)
	if err != nil {
		t.Fatalf("Unexpected failure setting up test connection: %s", err.Error())
	}
	err = client.Connect()
	if err != nil {
		t.Fatalf("Unexpected failure connecting: %s", err.Error())
	}
	return client
}

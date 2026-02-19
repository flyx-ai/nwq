package main_test

import (
	"context"
	"testing"

	"github.com/flyx-ai/nwq/client"
	"github.com/flyx-ai/nwq/internal/testtasks"
	"github.com/nats-io/nats.go"
)

type DoThingMessage struct {
	ThingID string `json:"thing_id"`
}

type DoOtherThingMessage struct {
	ThingID      string `json:"thing_id"`
	OtherThingID string `json:"other_thing_id"`
}

func TestNWQ(t *testing.T) {
	nc, err := nats.Connect(nats.DefaultURL)
	if err != nil {
		t.Fatalf("failed to connect to nats: %v", err)
	}

	err = client.Initialize(context.Background(), nc)
	if err != nil {
		t.Fatalf("failed to initialize nwq client: %v", err)
	}

	err = testtasks.DoThingTask.Run(context.Background(), testtasks.DoThingMessage{ThingID: "123"})
	if err != nil {
		t.Fatalf("failed to run do thing task: %v", err)
	}
}

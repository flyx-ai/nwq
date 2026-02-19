package client

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/synadia-io/orbit.go/counters"
)

var (
	NC *nats.Conn
	JS jetstream.JetStream

	TaskStream          jetstream.Stream
	CounterStream       jetstream.Stream
	Counter             counters.Counter
	WorkflowObjectStore jetstream.ObjectStore
	WorkflowKV          jetstream.KeyValue
)

func Initialize(ctx context.Context, nc *nats.Conn) error {
	err := initialize(ctx, nc, 3)
	var apiErr *jetstream.APIError
	if errors.As(err, &apiErr) {
		if apiErr.ErrorCode == 10074 {
			return initialize(ctx, nc, 1)
		}
	}
	return err
}

func initialize(ctx context.Context, nc *nats.Conn, numReplicas int) error {
	var err error
	NC = nc
	JS, err = jetstream.New(nc)
	if err != nil {
		return fmt.Errorf("failed to create JetStream context: %w", err)
	}

	TaskStream, err = JS.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:               "nwq-tasks",
		Description:        "Stream for NWQ tasks",
		Subjects:           []string{"nwq.messages.>"},
		Retention:          jetstream.InterestPolicy,
		Discard:            jetstream.DiscardNew,
		Storage:            jetstream.FileStorage,
		Replicas:           numReplicas,
		AllowDirect:        true,
		AllowMsgTTL:        true,
		AllowAtomicPublish: true,
		AllowMsgSchedules:  true,
	})
	if err != nil {
		return fmt.Errorf("failed to create or update TaskStream: %w", err)
	}

	CounterStream, err = JS.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:            "nwq-counters",
		Description:     "Stream for NWQ counters",
		Subjects:        []string{"nwq.counters.*"},
		Storage:         jetstream.FileStorage,
		Replicas:        numReplicas,
		AllowMsgCounter: true,
		AllowDirect:     true,
	})
	if err != nil {
		return fmt.Errorf("failed to create or update CounterStream: %w", err)
	}

	WorkflowObjectStore, err = JS.CreateOrUpdateObjectStore(ctx, jetstream.ObjectStoreConfig{
		Bucket:      "nwq-workflow-objects",
		Description: "Object store for NWQ workflow data",
		TTL:         24 * 7 * time.Hour,
		Storage:     jetstream.FileStorage,
		Replicas:    numReplicas,
	})
	if err != nil {
		return fmt.Errorf("failed to create or update WorkflowObjectStore: %w", err)
	}

	WorkflowKV, err = JS.CreateOrUpdateKeyValue(ctx, jetstream.KeyValueConfig{
		Bucket:      "nwq-workflow-kv",
		Description: "KeyValue store for NWQ workflow data",
		TTL:         24 * 7 * time.Hour,
		Storage:     jetstream.FileStorage,
		Replicas:    numReplicas,
	})
	if err != nil {
		return fmt.Errorf("failed to create or update WorkflowKV: %w", err)
	}

	Counter, err = counters.NewCounterFromStream(JS, CounterStream)
	if err != nil {
		return fmt.Errorf("failed to create counter from TaskStream: %w", err)
	}

	return nil
}

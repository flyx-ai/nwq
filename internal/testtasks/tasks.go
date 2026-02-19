package testtasks

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"time"

	"github.com/flyx-ai/nwq/task"
	"github.com/flyx-ai/nwq/workflow"
)

type DoThingMessage struct {
	ThingID string `json:"thing_id"`
}

type DoOtherThingMessage struct {
	ThingID      string `json:"thing_id"`
	OtherThingID string `json:"other_thing_id"`
}

var firstDoThing = false

var DoThingTask = task.NewTask("do-thing", "v1", func(ctx context.Context, msg DoThingMessage) error {
	// this simulates a failure on the first attempt, which will cause a retry after 1 second
	if !firstDoThing {
		firstDoThing = true
		return fmt.Errorf("first do thing failed")
	}

	// storeobject stores a reader into the nats object store, this is slower than kv, but can store larger objects
	// and doesn't require the entire value to be loaded into memory at once
	err := workflow.StoreObject(ctx, "thing_file", bytes.NewReader([]byte("file contents")))
	if err != nil {
		return fmt.Errorf("failed to store object: %w", err)
	}

	// storekv stores a byte slice into the nats key value store, this is a lot faster than object store, but is limited in size.
	// (it's not documented but I'm guessing it's 1mb - same as the size limit for normal messages)
	// you can have a large number of small kv entries though, which would be more efficient than object store.
	// both object and kv stores are automatically cleaned up after the workflow finishes.
	err = workflow.StoreKV(ctx, "thing_kv", []byte("kv value"))
	if err != nil {
		return fmt.Errorf("failed to store kv: %w", err)
	}

	slog.Info("doing thing", "thing_id", msg.ThingID)

	// you can execute an arbitrary number of subtasks from a task. It is possible for the same task to be
	// executed multiple times, so make sure that the tasks are idempotent or that you handle duplicates appropriately.
	// Tasks are executed asynchronously, so the parent task will not wait for them to complete before finishing.
	err = DoOtherThingTask.Run(ctx, DoOtherThingMessage{ThingID: msg.ThingID, OtherThingID: "456"})
	if err != nil {
		return fmt.Errorf("failed to run do other thing task: %w", err)
	}

	err = DoOtherThingTask.Run(ctx, DoOtherThingMessage{ThingID: msg.ThingID, OtherThingID: "789"})
	if err != nil {
		return fmt.Errorf("failed to run do other thing task: %w", err)
	}

	return nil
	// The retry can be configured on a per task basis, and can be set to any duration. The default is 5 retries with exponential backoff and jitter, starting at 1 second.
}, time.Minute)

var DoOtherThingTask = task.NewTask("do-other-thing", "v1", func(ctx context.Context, msg DoOtherThingMessage) error {
	slog.Info("doing other thing", "thing_id", msg.ThingID, "other_thing_id", msg.OtherThingID)

	obj, err := workflow.GetObject(ctx, "thing_file")
	if err != nil {
		return fmt.Errorf("failed to get object: %w", err)
	}

	// Make sure that all objects are closed after use to prevent resource leaks.
	defer func() {
		err := obj.Close()
		if err != nil {
			slog.Error("failed to close object", "error", err)
		}
	}()
	objContent, err := io.ReadAll(obj)
	if err != nil {
		return fmt.Errorf("failed to read object content: %w", err)
	}

	slog.Info("got object", "content", string(objContent))

	kvValue, err := workflow.GetKV(ctx, "thing_kv")
	if err != nil {
		return fmt.Errorf("failed to get kv: %w", err)
	}

	slog.Info("got kv", "value", string(kvValue))

	return nil
}, time.Minute)

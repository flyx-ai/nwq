package workflow

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/flyx-ai/nwq/client"
	"github.com/flyx-ai/nwq/task"
	"github.com/nats-io/nats.go/jetstream"
)

func StoreObject(ctx context.Context, key string, reader io.Reader) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	workflowID, ok := task.GetWorkflowID(ctx)
	if !ok {
		return fmt.Errorf("workflow ID not found in context")
	}

	wg := sync.WaitGroup{}
	wg.Add(1)

	var innerErr error

	go func() {
		defer wg.Done()

		_, innerErr = client.WorkflowKV.Create(ctx, workflowID+".NWQOS."+key, nil)
		if innerErr != nil {
			cancel()
		}
	}()

	_, err := client.WorkflowObjectStore.Put(ctx, jetstream.ObjectMeta{
		Name: workflowID + "." + key,
	}, reader)
	if err != nil {
		if errors.Is(err, context.Canceled) && innerErr != nil {
			return fmt.Errorf("failed to create key in WorkflowKV: %w", innerErr)
		}

		return fmt.Errorf("failed to put object in WorkflowObjectStore: %w", err)
	}

	wg.Wait()

	if innerErr != nil {
		return fmt.Errorf("failed to create key in WorkflowKV: %w", innerErr)
	}

	return nil
}

func GetObject(ctx context.Context, key string) (jetstream.ObjectResult, error) {
	workflowID, ok := task.GetWorkflowID(ctx)
	if !ok {
		return nil, fmt.Errorf("workflow ID not found in context")
	}

	result, err := client.WorkflowObjectStore.Get(ctx, workflowID+"."+key)
	if err != nil {
		return nil, fmt.Errorf("failed to get object from WorkflowObjectStore: %w", err)
	}

	return result, nil
}

func StoreKV(ctx context.Context, key string, value []byte) error {
	workflowID, ok := task.GetWorkflowID(ctx)
	if !ok {
		return fmt.Errorf("workflow ID not found in context")
	}

	_, err := client.WorkflowKV.Put(ctx, workflowID+"."+key, value)
	if err != nil {
		return fmt.Errorf("failed to put key-value pair in WorkflowKV: %w", err)
	}

	return nil
}

func GetKV(ctx context.Context, key string) ([]byte, error) {
	workflowID, ok := task.GetWorkflowID(ctx)
	if !ok {
		return nil, fmt.Errorf("workflow ID not found in context")
	}

	entry, err := client.WorkflowKV.Get(ctx, workflowID+"."+key)
	if err != nil {
		return nil, fmt.Errorf("failed to get key-value pair from WorkflowKV: %w", err)
	}

	return entry.Value(), nil
}

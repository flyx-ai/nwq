package task

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/flyx-ai/nwq/client"
	"github.com/nats-io/nats.go/jetstream"
)

type workflowCompletionMessage struct {
	WorkflowID string `json:"workflow_id"`
}

var WorkflowCompletionTask = NewTask(
	"workflow-completion",
	"v1",
	func(ctx context.Context, input workflowCompletionMessage) error {
		keyLister, err := client.WorkflowKV.ListKeysFiltered(ctx, input.WorkflowID+".>")
		if err != nil {
			return fmt.Errorf("failed to list keys in WorkflowKV: %w", err)
		}

		wg := sync.WaitGroup{}
		errChan := make(chan error)
		for key := range keyLister.Keys() {
			newKey := strings.TrimPrefix(key, input.WorkflowID+".NWQOS.")
			if len(newKey) != len(key) {
				wg.Go(func() {
					err := client.WorkflowObjectStore.Delete(ctx, input.WorkflowID+"."+newKey)
					if err != nil {
						errChan <- fmt.Errorf("failed to delete object from WorkflowObjectStore: %w", err)
						return
					}

					err = client.WorkflowKV.Purge(ctx, key)
					if err != nil {
						errChan <- fmt.Errorf("failed to delete key from WorkflowKV: %w", err)
						return
					}
				})
			} else {
				wg.Go(func() {
					err = client.WorkflowKV.Purge(ctx, key)
					if err != nil {
						errChan <- fmt.Errorf("failed to delete key from WorkflowKV: %w", err)
						return
					}
				})
			}
		}

		wg.Go(func() {
			err = client.CounterStream.Purge(ctx, jetstream.WithPurgeSubject("nwq.counters."+input.WorkflowID))
			if err != nil {
				errChan <- fmt.Errorf("failed to purge counter stream: %w", err)
				return
			}
		})

		go func() {
			wg.Wait()
			close(errChan)
		}()

		deleteErrors := make([]error, 0)
		for err := range errChan {
			if err != nil {
				deleteErrors = append(deleteErrors, err)
			}
		}

		if len(deleteErrors) > 0 {
			return errors.Join(deleteErrors...)
		}

		return nil
	},
	WithTimeout(time.Minute*5),
	WithRetry(DefaultRetryPolicy),
	withWorkflowCompletion(),
)

package task

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"strconv"
	"time"

	"github.com/flyx-ai/nwq/client"
	"github.com/google/uuid"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

type RetryPolicy struct {
	NumRetries int
	// Backoff is a function that takes the current attempt number (starting from 0) and returns the duration to wait before the next retry.
	Backoff func(attempt int) time.Duration
}

var DefaultRetryPolicy = RetryPolicy{
	NumRetries: 5,
	// Exponential backoff with jitter
	Backoff: func(attempt int) time.Duration {
		base := time.Second
		backoff := base * (1 << attempt) // Exponential backoff
		jitter := time.Duration(float64(backoff) * 0.1 * (0.5 - rand.Float64()))
		return backoff + jitter
	},
}

type WorkerTask interface {
	Handle(ctx context.Context, msg jetstream.Msg) error
	Name() string
	Version() string
	MessageSubject() string
}

type Task[Message any] struct {
	name                 string
	version              string
	handler              func(ctx context.Context, msg Message) error
	retry                RetryPolicy
	timeout              time.Duration
	isWorkflowCompletion bool
}

func NewTask[Message any](name string, version string, handler func(ctx context.Context, msg Message) error, timeout time.Duration) Task[Message] {
	return Task[Message]{
		name:    name,
		version: version,
		handler: handler,
		retry:   DefaultRetryPolicy,
		timeout: timeout,
	}
}

func NewTaskWithRetry[Message any](name string, version string, handler func(ctx context.Context, msg Message) error, retry RetryPolicy, timeout time.Duration) Task[Message] {
	return Task[Message]{
		name:    name,
		version: version,
		handler: handler,
		retry:   retry,
		timeout: timeout,
	}
}

func (t Task[Message]) Name() string {
	return t.name
}

func (t Task[Message]) Version() string {
	return t.version
}

var _ WorkerTask = (*Task[any])(nil)

type workflowIDContextKey struct{}

func WithWorkflowID(ctx context.Context, workflowID string) context.Context {
	return context.WithValue(ctx, workflowIDContextKey{}, workflowID)
}

func GetWorkflowID(ctx context.Context) (string, bool) {
	workflowID, ok := ctx.Value(workflowIDContextKey{}).(string)
	return workflowID, ok
}

func counterSubject(workflowID string) string {
	return "nwq.counters." + workflowID
}

func (t Task[Message]) MessageSubject() string {
	return "nwq.messages." + t.version + t.name
}

func (t Task[Message]) Run(ctx context.Context, msg Message) error {
	slog.Info("running task", "taskName", t.name)

	workflowID, ok := GetWorkflowID(ctx)
	if !ok {
		workflowID = uuid.New().String()
		ctx = WithWorkflowID(ctx, workflowID)
	}

	if !t.isWorkflowCompletion {
		_, err := client.Counter.AddInt(ctx, counterSubject(workflowID), 1)
		if err != nil {
			return fmt.Errorf("failed to increment counter: %w", err)
		}
	}

	marshalledInput, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("failed to marshal message: %w", err)
	}

	message := nats.NewMsg(t.MessageSubject())
	message.Data = marshalledInput
	message.Header.Set("X-NWQ-Workflow-ID", workflowID)
	message.Header.Set("X-NWQ-Task-Timeout", strconv.FormatInt(t.timeout.Nanoseconds(), 10))

	_, err = client.JS.PublishMsg(ctx, message)
	if err != nil {
		return fmt.Errorf("failed to publish task message: %w", err)
	}

	return nil
}

func (t Task[Message]) handleWorkflowCompletion(ctx context.Context, workflowID string) error {
	err := WorkflowCompletionTask.Run(ctx, workflowCompletionMessage{WorkflowID: workflowID})
	if err != nil {
		return fmt.Errorf("failed to run workflow completion task: %w", err)
	}

	return nil
}

func (t Task[Message]) Handle(ctx context.Context, msg jetstream.Msg) error {
	workflowID := msg.Headers().Get("X-NWQ-Workflow-ID")
	if workflowID == "" {
		return fmt.Errorf("missing workflow ID in message header")
	}

	taskTimeoutRaw := msg.Headers().Get("X-NWQ-Task-Timeout")
	taskTimeout := 30 * time.Second
	if taskTimeoutRaw != "" {
		timeoutInt, err := strconv.ParseInt(taskTimeoutRaw, 10, 64)
		if err != nil {
			slog.Warn("invalid task timeout in message header, using default", "error", err, "taskTimeout", taskTimeout)
		}
		taskTimeout = time.Duration(timeoutInt)
	} else {
		slog.Warn("missing task timeout in message header, using default")
	}

	if err := msg.InProgress(); err != nil {
		return fmt.Errorf("failed to signal in-progress for message: %w", err)
	}

	ctx, cancel := context.WithTimeoutCause(ctx, taskTimeout, fmt.Errorf("task %s timed out after %s", t.Name(), taskTimeout))
	defer cancel()

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-time.After(time.Second * 25):
				err := msg.InProgress()
				if err != nil {
					slog.Error("failed to send heartbeat for message", "error", err, "workflowID", workflowID)
				}
			}
		}
	}()

	ctx = WithWorkflowID(ctx, workflowID)

	var msgInput Message
	err := json.Unmarshal(msg.Data(), &msgInput)
	if err != nil {
		return fmt.Errorf("failed to unmarshal task input: %w", err)
	}

	err = t.handler(ctx, msgInput)
	if err != nil {
		meta, metaErr := msg.Metadata()
		if metaErr != nil {
			return fmt.Errorf("failed to get message metadata: %w", metaErr)
		}

		numDelivered := meta.NumDelivered

		slog.Error("task handler error", "error", err, "workflowID", workflowID, "attempt", numDelivered)

		if uint64(t.retry.NumRetries) > 0 && numDelivered > uint64(t.retry.NumRetries) {
			err := msg.Term()
			if err != nil {
				return fmt.Errorf("failed to terminate message after exceeding retry limit for attempt %d: %w", numDelivered, err)
			}

			if !t.isWorkflowCompletion {
				newVal, err := client.Counter.AddInt(ctx, counterSubject(workflowID), -1)
				if err != nil {
					return fmt.Errorf("failed to decrement counter after exceeding retry limit for attempt %d: %w", numDelivered, err)
				}

				if newVal.Int64() == 0 {
					err = t.handleWorkflowCompletion(ctx, workflowID)
					if err != nil {
						return fmt.Errorf("failed to handle workflow completion after exceeding retry limit for attempt %d: %w", numDelivered, err)
					}
				}
			}
		} else {
			err = msg.NakWithDelay(t.retry.Backoff(int(numDelivered - 1)))
			if err != nil {
				return fmt.Errorf("failed to nack message for attempt %d: %w", numDelivered, err)
			}
		}
	} else {
		err := msg.Ack()
		if err != nil {
			return fmt.Errorf("failed to ack message: %w", err)
		}

		if !t.isWorkflowCompletion {
			newVal, err := client.Counter.AddInt(ctx, counterSubject(workflowID), -1)
			if err != nil {
				return fmt.Errorf("failed to decrement counter: %w", err)
			}

			if newVal.Int64() == 0 {
				err = t.handleWorkflowCompletion(ctx, workflowID)
				if err != nil {
					return fmt.Errorf("failed to handle workflow completion: %w", err)
				}
			}
		}
	}

	return nil
}

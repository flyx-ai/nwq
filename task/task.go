package task

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"time"

	"github.com/cespare/xxhash/v2"
	"github.com/flyx-ai/nwq/client"
	"github.com/flyx-ai/nwq/task/taskinfo"
	"github.com/flyx-ai/nwq/workflow"
	"github.com/google/uuid"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

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
	isWorkflowCompletion bool
	to                   taskOptions
}

func NewTask[Message any](
	name string,
	version string,
	handler func(ctx context.Context, msg Message) error,
	options ...TaskOption,
) Task[Message] {
	task := Task[Message]{
		name:    name,
		version: version,
		handler: handler,
		to: taskOptions{
			retry:   DefaultRetryPolicy,
			timeout: 30 * time.Second,
		},
	}

	for _, option := range options {
		option.apply(&task.to)
	}

	return task
}

func (t Task[Message]) Name() string {
	return t.name
}

func (t Task[Message]) Version() string {
	return t.version
}

var _ WorkerTask = (*Task[any])(nil)

func counterSubject(workflowID string) string {
	return "nwq.counters." + workflowID
}

func (t Task[Message]) MessageSubject() string {
	return "nwq.messages." + t.version + "." + t.name
}

func (t Task[Message]) ScheduleSubject() string {
	return "nwq.schedules." + t.version + "." + t.name
}

func (t Task[Message]) getHash(workflowID string, rawMsg []byte) uint64 {
	hash := xxhash.New()
	_, _ = hash.Write([]byte(workflowID))
	_, _ = hash.Write([]byte("NWQ_SEP"))
	_, _ = hash.Write([]byte(t.name))
	_, _ = hash.Write([]byte("NWQ_SEP"))
	_, _ = hash.Write([]byte(t.version))
	_, _ = hash.Write([]byte("NWQ_SEP"))
	_, _ = hash.Write(rawMsg)
	return hash.Sum64()
}

func (t Task[Message]) Run(ctx context.Context, msg Message) error {
	slog.Info("running task", "taskName", t.name)

	workflowID, ok := taskinfo.GetWorkflowID(ctx)
	if !ok {
		workflowID = uuid.New().String()
		ctx = taskinfo.WithWorkflowID(ctx, workflowID)
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
	message.Header.Set("X-NWQ-Task-Timeout", strconv.FormatInt(int64(t.to.timeout), 10))

	if t.to.dedup {
		dedupIDRaw := t.getHash(workflowID, marshalledInput)
		dedupID := strconv.FormatUint(dedupIDRaw, 16)
		message.Header.Set("Nats-Msg-Id", dedupID)
	}

	_, err = client.JS.PublishMsg(ctx, message)
	if err != nil {
		return fmt.Errorf("failed to publish task message: %w", err)
	}

	return nil
}

var ErrDuplicateScheduledTask = fmt.Errorf("duplicate scheduled task")

func (t Task[Message]) Schedule(
	ctx context.Context,
	msg Message,
	schedule string,
	dedupID string,
	overwriteExisting bool,
) error {
	slog.Info("running scheduled task", "taskName", t.name)

	if dedupID == "" {
		dedupID = t.ScheduleSubject()
	}

	dedupKey := "NWQ_SCHEDULED_" + dedupID

	marshalledInput, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("failed to marshal message: %w", err)
	}

	message := nats.NewMsg(t.ScheduleSubject())
	message.Data = marshalledInput
	message.Header.Set("X-NWQ-Task-Timeout", strconv.FormatInt(int64(t.to.timeout), 10))
	message.Header.Set("Nats-Schedule", schedule)
	message.Header.Set("Nats-Schedule-Target", t.MessageSubject())

	_, err = client.WorkflowKV.Create(ctx, dedupKey, nil)
	if errors.Is(err, jetstream.ErrKeyExists) {
		if overwriteExisting {
			slog.Info(
				"scheduled task with same deduplication ID already exists, purging existing messages and overwriting",
				"dedupKey",
				dedupKey,
			)
			dedupKeyLock := dedupKey + "_LOCK"
			for {
				_, err := client.WorkflowKV.Create(ctx, dedupKeyLock, nil, jetstream.KeyTTL(time.Minute))
				if errors.Is(err, jetstream.ErrKeyExists) {
					continue
				}

				defer func() {
					err := client.WorkflowKV.Purge(ctx, dedupKeyLock)
					if err != nil {
						slog.Error(
							"failed to release lock for scheduled task deduplication key",
							"error",
							err,
							"dedupKeyLock",
							dedupKeyLock,
						)
					}
				}()

				err = client.TaskStream.Purge(ctx, jetstream.WithPurgeSubject(t.ScheduleSubject()))
				if err != nil {
					return fmt.Errorf("failed to purge existing scheduled task messages: %w", err)
				}

				slog.Info(
					"publishing new scheduled task message after purging existing messages",
					"dedupKey",
					dedupKey,
				)

				_, err = client.JS.PublishMsg(ctx, message)
				if err != nil {
					return fmt.Errorf("failed to publish task message: %w", err)
				}

				return nil
			}
		} else {
			return ErrDuplicateScheduledTask
		}
	} else if err != nil {
		return fmt.Errorf("failed to create deduplication key for scheduled task: %w", err)
	}

	slog.Info(
		"publishing scheduled task message",
		"dedupKey",
		dedupKey,
	)

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

	needAdd := false

	if workflowID == "" {
		if msg.Headers().Get("Nats-Scheduler") != "" {
			// This is a scheduled message, which does not contain a workflow ID.
			// We will manually assign a workflow ID for it.
			workflowID = uuid.New().String()
			needAdd = true
		} else {
			return fmt.Errorf("missing workflow ID in message header")
		}
	}

	ctx = taskinfo.WithWorkflowID(ctx, workflowID)

	// This is a scheduled message that requires an increment for the workflow counter.
	// We use a KV entry as a lock to ensure that only one message increments the counter for this workflow,
	if needAdd {
		err := workflow.CreateKV(ctx, "NWQ_SCHEDULED_INCREMENTED", nil)
		if err != nil {
			if !errors.Is(err, jetstream.ErrKeyExists) {
				return fmt.Errorf("failed to create workflow KV for scheduled task: %w", err)
			}
		} else {
			_, err := client.Counter.AddInt(ctx, counterSubject(workflowID), 1)
			if err != nil {
				kvErr := client.WorkflowKV.Delete(ctx, "NWQ_SCHEDULED_INCREMENTED")
				if kvErr != nil {
					return fmt.Errorf(
						"failed to delete workflow KV after counter increment failure for scheduled task: %w => %w",
						err,
						kvErr,
					)
				}

				return fmt.Errorf("failed to increment counter for scheduled task: %w", err)
			}
		}
	}

	if t.to.dedup {
		dedupIDRaw := t.getHash(workflowID, msg.Data())
		dedupID := strconv.FormatUint(dedupIDRaw, 16)

		err := workflow.CreateKV(ctx, "NWQ_DEDUP_"+dedupID, nil)
		if errors.Is(err, jetstream.ErrKeyExists) {
			slog.Info(
				"duplicate message detected, acknowledging without processing",
				"workflowID",
				workflowID,
				"dedupID",
				dedupID,
			)
			if err := msg.Ack(); err != nil {
				return fmt.Errorf("failed to ack duplicate message: %w", err)
			}
			return nil
		} else if err != nil {
			return fmt.Errorf("failed to create deduplication key: %w", err)
		}
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

		if uint64(t.to.retry.NumRetries) > 0 && numDelivered > uint64(t.to.retry.NumRetries) {
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
			err = msg.NakWithDelay(t.to.retry.Backoff(int(numDelivered - 1)))
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

package cron

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strconv"
	"sync"
	"time"

	"github.com/flyx-ai/nwq/client"
	"github.com/flyx-ai/nwq/task"
	"github.com/google/uuid"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	robfigcron "github.com/robfig/cron/v3"
)

type Scheduler struct {
	cron *robfigcron.Cron
	mu   sync.Mutex
	// maps KV key -> robfig entry ID for dynamic crons so we can remove them
	dynamicEntries map[string]robfigcron.EntryID
	watcher        jetstream.KeyWatcher
}

func NewScheduler() *Scheduler {
	return &Scheduler{
		cron:           robfigcron.New(),
		dynamicEntries: make(map[string]robfigcron.EntryID),
	}
}

// AddStaticCrons registers cron schedules defined on task definitions.
func (s *Scheduler) AddStaticCrons(tasks []task.WorkerTask) error {
	for _, t := range tasks {
		crons := t.OnCrons()
		if len(crons) == 0 {
			continue
		}
		for _, expr := range crons {
			cronName := t.Name() + "-static"
			subject := t.MessageSubject()
			timeout := t.Timeout()
			_, err := s.cron.AddFunc(expr, func() {
				if err := publish(subject, cronName, nil, timeout); err != nil {
					slog.Error("failed to publish static cron message", "task", subject, "error", err)
				}
			})
			if err != nil {
				return fmt.Errorf("invalid cron expression %q for task %s: %w", expr, t.Name(), err)
			}
			slog.Info("registered static cron", "task", t.Name(), "expression", expr)
		}
	}
	return nil
}

// WatchDynamicCrons starts a KV watcher on the nwq-crons bucket and
// adds/removes scheduler entries as cron definitions are created or deleted.
func (s *Scheduler) WatchDynamicCrons(ctx context.Context) error {
	watcher, err := client.CronKV.WatchAll(ctx)
	if err != nil {
		return fmt.Errorf("failed to start cron KV watcher: %w", err)
	}
	s.watcher = watcher

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case entry, ok := <-watcher.Updates():
				if !ok {
					return
				}
				if entry == nil {
					continue
				}
				s.handleKVUpdate(entry)
			}
		}
	}()

	return nil
}

func (s *Scheduler) handleKVUpdate(entry jetstream.KeyValueEntry) {
	key := entry.Key()

	switch entry.Operation() {
	case jetstream.KeyValuePut:
		var trigger cronKVValue
		if err := json.Unmarshal(entry.Value(), &trigger); err != nil {
			slog.Error("failed to unmarshal cron KV entry", "key", key, "error", err)
			return
		}

		s.mu.Lock()
		if existingID, exists := s.dynamicEntries[key]; exists {
			s.cron.Remove(existingID)
		}
		s.mu.Unlock()

		subject := trigger.TaskSubject
		cronName := trigger.Name
		input := trigger.Input
		timeout := time.Duration(trigger.TimeoutNanos)

		entryID, err := s.cron.AddFunc(trigger.Expression, func() {
			if err := publish(subject, cronName, input, timeout); err != nil {
				slog.Error("failed to publish dynamic cron message", "cron", cronName, "error", err)
			}
		})
		if err != nil {
			slog.Error("invalid cron expression in KV", "key", key, "expression", trigger.Expression, "error", err)
			return
		}

		s.mu.Lock()
		s.dynamicEntries[key] = entryID
		s.mu.Unlock()

		slog.Info("added dynamic cron", "key", key, "expression", trigger.Expression)

	case jetstream.KeyValueDelete, jetstream.KeyValuePurge:
		s.mu.Lock()
		if entryID, exists := s.dynamicEntries[key]; exists {
			s.cron.Remove(entryID)
			delete(s.dynamicEntries, key)
		}
		s.mu.Unlock()

		slog.Info("removed dynamic cron", "key", key)
	}
}

func (s *Scheduler) Start() {
	s.cron.Start()
}

func (s *Scheduler) Stop() {
	ctx := s.cron.Stop()
	<-ctx.Done()

	if s.watcher != nil {
		_ = s.watcher.Stop()
	}
}

// publish sends a task message to the nwq-tasks stream with a deterministic
// Nats-Msg-Id for cross-worker deduplication.
func publish(taskSubject string, cronName string, input json.RawMessage, timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	workflowID := uuid.New().String()

	if _, err := client.Counter.AddInt(ctx, "nwq.counters."+workflowID, 1); err != nil {
		return fmt.Errorf("failed to increment counter for cron workflow: %w", err)
	}

	data := input
	if data == nil {
		data = []byte("{}")
	}

	dedupID := fmt.Sprintf("cron-%s-%d", cronName, time.Now().Truncate(time.Minute).Unix())

	msg := nats.NewMsg(taskSubject)
	msg.Data = data
	msg.Header.Set("Nats-Msg-Id", dedupID)
	msg.Header.Set("X-NWQ-Workflow-ID", workflowID)
	msg.Header.Set("X-NWQ-Task-Timeout", strconv.FormatInt(timeout.Nanoseconds(), 10))

	if _, err := client.JS.PublishMsg(ctx, msg); err != nil {
		return fmt.Errorf("failed to publish cron message: %w", err)
	}

	slog.Info("published cron message", "subject", taskSubject, "cronName", cronName, "dedupID", dedupID)
	return nil
}

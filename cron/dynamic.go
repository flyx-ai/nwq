package cron

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/flyx-ai/nwq/client"
	robfigcron "github.com/robfig/cron/v3"
)

// cronKVValue is the JSON structure stored in the nwq-crons KV bucket.
type cronKVValue struct {
	Name         string            `json:"name"`
	Expression   string            `json:"expression"`
	TaskSubject  string            `json:"task_subject"`
	Input        json.RawMessage   `json:"input,omitempty"`
	Metadata     map[string]string `json:"metadata,omitempty"`
	TimeoutNanos int64             `json:"timeout_nanos"`
}

type CronTrigger struct {
	Name       string
	Expression string
	Input      json.RawMessage
	Metadata   map[string]string
	Timeout    time.Duration
}

type CronEntry struct {
	CronTrigger
	ID          string
	TaskSubject string
}

func kvKey(taskSubject, cronName string) string {
	return taskSubject + "." + cronName
}

// Create validates the cron expression and stores a new cron trigger in the KV bucket.
// The task subject should come from task.MessageSubject().
func Create(ctx context.Context, taskSubject string, trigger CronTrigger) (*CronEntry, error) {
	parser := robfigcron.NewParser(robfigcron.Minute | robfigcron.Hour | robfigcron.Dom | robfigcron.Month | robfigcron.Dow)
	if _, err := parser.Parse(trigger.Expression); err != nil {
		return nil, fmt.Errorf("invalid cron expression %q: %w", trigger.Expression, err)
	}

	value := cronKVValue{
		Name:         trigger.Name,
		Expression:   trigger.Expression,
		TaskSubject:  taskSubject,
		Input:        trigger.Input,
		Metadata:     trigger.Metadata,
		TimeoutNanos: trigger.Timeout.Nanoseconds(),
	}

	data, err := json.Marshal(value)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal cron trigger: %w", err)
	}

	key := kvKey(taskSubject, trigger.Name)
	if _, err := client.CronKV.Put(ctx, key, data); err != nil {
		return nil, fmt.Errorf("failed to store cron trigger in KV: %w", err)
	}

	return &CronEntry{
		CronTrigger: trigger,
		ID:          key,
		TaskSubject: taskSubject,
	}, nil
}

// Delete removes a cron trigger from the KV bucket.
func Delete(ctx context.Context, taskSubject string, cronName string) error {
	key := kvKey(taskSubject, cronName)
	if err := client.CronKV.Purge(ctx, key); err != nil {
		return fmt.Errorf("failed to delete cron trigger from KV: %w", err)
	}
	return nil
}

// List returns all cron triggers for a given task subject.
func List(ctx context.Context, taskSubject string) ([]CronEntry, error) {
	keys, err := client.CronKV.ListKeysFiltered(ctx, taskSubject+".>")
	if err != nil {
		return nil, fmt.Errorf("failed to list cron keys: %w", err)
	}

	var entries []CronEntry
	for key := range keys.Keys() {
		kve, err := client.CronKV.Get(ctx, key)
		if err != nil {
			return nil, fmt.Errorf("failed to get cron KV entry %s: %w", key, err)
		}

		var value cronKVValue
		if err := json.Unmarshal(kve.Value(), &value); err != nil {
			return nil, fmt.Errorf("failed to unmarshal cron KV entry %s: %w", key, err)
		}

		entries = append(entries, CronEntry{
			CronTrigger: CronTrigger{
				Name:       value.Name,
				Expression: value.Expression,
				Input:      value.Input,
				Metadata:   value.Metadata,
				Timeout:    time.Duration(value.TimeoutNanos),
			},
			ID:          key,
			TaskSubject: value.TaskSubject,
		})
	}

	return entries, nil
}

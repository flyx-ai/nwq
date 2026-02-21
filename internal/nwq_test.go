package main_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/flyx-ai/nwq/client"
	"github.com/flyx-ai/nwq/cron"
	"github.com/flyx-ai/nwq/internal/testtasks"
	"github.com/flyx-ai/nwq/worker"
	"github.com/nats-io/nats.go"
)

type DoThingMessage struct {
	ThingID string `json:"thing_id"`
}

type DoOtherThingMessage struct {
	ThingID      string `json:"thing_id"`
	OtherThingID string `json:"other_thing_id"`
}

func initClient(t *testing.T) *nats.Conn {
	t.Helper()
	nc, err := nats.Connect(nats.DefaultURL)
	if err != nil {
		t.Fatalf("failed to connect to nats: %v", err)
	}
	if err := client.Initialize(context.Background(), nc); err != nil {
		t.Fatalf("failed to initialize nwq client: %v", err)
	}
	return nc
}

func TestNWQ(t *testing.T) {
	initClient(t)

	err := testtasks.DoThingTask.Run(context.Background(), testtasks.DoThingMessage{ThingID: "123"})
	if err != nil {
		t.Fatalf("failed to run do thing task: %v", err)
	}
}

// TestStaticCron registers a cron task with "* * * * *" (every minute),
// starts a worker, waits for the cron to fire, and verifies the handler was called.
func TestStaticCron(t *testing.T) {
	initClient(t)

	testtasks.CleanupCallCount.Store(0)

	w := worker.NewWorker()
	w.RegisterTask(testtasks.CleanupTask)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := w.Start(ctx); err != nil {
		t.Fatalf("failed to start worker: %v", err)
	}
	defer w.Stop(ctx)

	// robfig/cron fires at the top of the next minute; wait up to 65s
	deadline := time.After(65 * time.Second)
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-deadline:
			t.Fatalf("timed out waiting for static cron to fire; call count: %d", testtasks.CleanupCallCount.Load())
		case <-ticker.C:
			if testtasks.CleanupCallCount.Load() > 0 {
				t.Logf("static cron fired %d time(s)", testtasks.CleanupCallCount.Load())
				return
			}
		}
	}
}

// TestDynamicCronCreateDelete verifies that dynamic cron triggers can be
// created, listed, and deleted via the cron CRUD API.
func TestDynamicCronCreateDelete(t *testing.T) {
	initClient(t)

	ctx := context.Background()
	subject := testtasks.ReportTask.MessageSubject()

	inputJSON, _ := json.Marshal(testtasks.ReportMessage{CustomerID: "customer-a"})

	entry, err := cron.Create(ctx, subject, cron.CronTrigger{
		Name:       "daily-report",
		Expression: "0 12 * * *",
		Input:      inputJSON,
		Metadata:   map[string]string{"customer_id": "customer-a"},
		Timeout:    time.Minute,
	})
	if err != nil {
		t.Fatalf("failed to create cron: %v", err)
	}
	t.Logf("created cron: %s", entry.ID)

	entries, err := cron.List(ctx, subject)
	if err != nil {
		t.Fatalf("failed to list crons: %v", err)
	}
	if len(entries) == 0 {
		t.Fatal("expected at least 1 cron entry, got 0")
	}

	found := false
	for _, e := range entries {
		if e.Name == "daily-report" && e.Expression == "0 12 * * *" {
			found = true
			break
		}
	}
	if !found {
		t.Fatal("created cron not found in list")
	}

	if err := cron.Delete(ctx, subject, "daily-report"); err != nil {
		t.Fatalf("failed to delete cron: %v", err)
	}

	entries, err = cron.List(ctx, subject)
	if err != nil {
		t.Fatalf("failed to list crons after delete: %v", err)
	}
	for _, e := range entries {
		if e.Name == "daily-report" {
			t.Fatal("cron still present after deletion")
		}
	}
}

// TestCronDeduplication starts two workers with the same static cron task and
// verifies the handler fires exactly once per cron tick (not twice) thanks to
// JetStream message deduplication.
func TestCronDeduplication(t *testing.T) {
	initClient(t)

	testtasks.CleanupCallCount.Store(0)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	w1 := worker.NewWorker()
	w1.RegisterTask(testtasks.CleanupTask)
	if err := w1.Start(ctx); err != nil {
		t.Fatalf("failed to start worker 1: %v", err)
	}
	defer w1.Stop(ctx)

	w2 := worker.NewWorker()
	w2.RegisterTask(testtasks.CleanupTask)
	if err := w2.Start(ctx); err != nil {
		t.Fatalf("failed to start worker 2: %v", err)
	}
	defer w2.Stop(ctx)

	// Wait for one cron tick (up to 65s), then give a few extra seconds for
	// any duplicate to arrive.
	deadline := time.After(65 * time.Second)
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-deadline:
			t.Fatalf("timed out waiting for cron to fire; call count: %d", testtasks.CleanupCallCount.Load())
		case <-ticker.C:
			if testtasks.CleanupCallCount.Load() > 0 {
				// Wait a few seconds for any potential duplicate to be processed.
				time.Sleep(5 * time.Second)
				count := testtasks.CleanupCallCount.Load()
				if count != 1 {
					t.Fatalf("expected cron to fire exactly once, but got %d executions", count)
				}
				t.Log("cron fired exactly once across 2 workers (dedup works)")
				return
			}
		}
	}
}

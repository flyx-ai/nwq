package testtasks

import (
	"context"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/flyx-ai/nwq/task"
)

type CleanupMessage struct{}

// CleanupCallCount tracks how many times CleanupTask has been invoked (for tests).
var CleanupCallCount atomic.Int64

var CleanupTask = task.NewCronTask("cleanup", "v1", func(ctx context.Context, msg CleanupMessage) error {
	CleanupCallCount.Add(1)
	slog.Info("running scheduled cleanup")
	return nil
}, []string{"* * * * *"}, time.Minute)

type ReportMessage struct {
	CustomerID string `json:"customer_id"`
}

// ReportCallCount tracks how many times ReportTask has been invoked (for tests).
var ReportCallCount atomic.Int64

// ReportTask has no static cron — it's meant to be triggered by dynamic crons created at runtime.
var ReportTask = task.NewTask("report", "v1", func(ctx context.Context, msg ReportMessage) error {
	ReportCallCount.Add(1)
	slog.Info("generating report", "customer_id", msg.CustomerID)
	return nil
}, time.Minute)

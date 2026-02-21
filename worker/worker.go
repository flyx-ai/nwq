package worker

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/flyx-ai/nwq/client"
	"github.com/flyx-ai/nwq/task"
	"github.com/nats-io/nats.go/jetstream"
)

type Worker struct {
	tasks           []task.WorkerTask
	consumers       []jetstream.Consumer
	consumeContexts []jetstream.ConsumeContext
}

func NewWorker() *Worker {
	worker := &Worker{}
	worker.RegisterTask(task.WorkflowCompletionTask)
	return worker
}

func (w *Worker) RegisterTask(task task.WorkerTask) {
	w.tasks = append(w.tasks, task)
}

func (w *Worker) Start(ctx context.Context) error {
	wg := sync.WaitGroup{}
	errChan := make(chan error)
	w.consumeContexts = make([]jetstream.ConsumeContext, 0, len(w.tasks))

	for _, t := range w.tasks {
		consumer, err := client.TaskStream.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{
			Name:          t.Name() + "-" + t.Version(),
			Durable:       t.Name() + "-" + t.Version(),
			Description:   "Consumer for task " + t.Name() + " version " + t.Version(),
			DeliverPolicy: jetstream.DeliverAllPolicy,
			AckPolicy:     jetstream.AckExplicitPolicy,
			AckWait:       30 * time.Second,
			// this is handled internally
			MaxDeliver: -1,
			// since we handle heartbeat internally, this should not be hit, but we set it just in case
			// the first value must be >=30 seconds to have a sensible AckWait time
			BackOff: []time.Duration{
				30 * time.Second,
				1 * time.Minute,
				5 * time.Minute,
				30 * time.Minute,
				1 * time.Hour,
				24 * time.Hour,
			},
			FilterSubject: t.MessageSubject(),
			ReplayPolicy:  jetstream.ReplayInstantPolicy,
		})
		if err != nil {
			return fmt.Errorf("failed to create or update consumer for task %s: %w", t.Name(), err)
		}

		w.consumers = append(w.consumers, consumer)
		wg.Add(1)

		go func() {
			consumeCtx, err := consumer.Consume(func(msg jetstream.Msg) {
				err := t.Handle(ctx, msg)
				if err != nil {
					slog.Error("failed to handle message", "task", t.Name(), "error", err)
				}
			})
			if err != nil {
				errChan <- fmt.Errorf("failed to start consumer for task %s: %w", t.Name(), err)
				wg.Done()
				return
			}
			wg.Done()
			w.consumeContexts = append(w.consumeContexts, consumeCtx)
		}()
	}

	go func() {
		wg.Wait()
		close(errChan)
	}()

	startErrors := make([]error, 0)
	for err := range errChan {
		if err != nil {
			startErrors = append(startErrors, err)
		}
	}

	if len(startErrors) > 0 {
		return fmt.Errorf("failed to start worker: %w", fmt.Errorf("%v", startErrors))
	}

	return nil
}

func (w *Worker) Stop(ctx context.Context) {
	wg := sync.WaitGroup{}
	for _, consumeCtx := range w.consumeContexts {
		wg.Go(func() {
			consumeCtx.Stop()
			<-consumeCtx.Closed()
		})
	}
	wg.Wait()
}
